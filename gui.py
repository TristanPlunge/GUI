import customtkinter as ctk
from tkinter import messagebox
from datetime import datetime, timedelta
import threading
import time

from widgets import CollapsibleSection
from plot_manager import PlotManager
from query_manager import QueryManager
import config_manager

from tksheet import Sheet


class MetricsApp:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.held_keys = set()
        self.modifiers = set()
        # Core state
        self.df = None
        self.query_manager = QueryManager(logger=self.log)
        self.plot_manager = None
        self.threads = []
        self.after_ids = []
        self.is_closing = False
        self.query_start_time = None
        self.timer_running = False

        # Master color map
        self.color_map = {
            "fan_tach_rpm": "#1E90FF",
            "coolant_temp_f": "#FF4500",
            "ebox_temp_f": "#FFD700",
            "water_temp_f": "#00CED1",
            "target_temp_f": "#ADFF2F",
            "flow_sense_lpm": "#00FF7F",
            "pump_current_amp": "#FF69B4",
            "compressor_current_amp": "#FFA500",
        }

        # Root window
        self.root = ctk.CTk()
        self.root.withdraw()
        self.root.title("Plunge Tub Metrics Analytics")

        # ✅ Load config
        self.config = config_manager.load_config()

        # Build UI
        self.build_output()
        self.build_controls()
        self.build_log()

        # Try to restore cached plot
        self.restore_cached_plot()

        # Grid weights
        # Root grid config
        self.root.grid_rowconfigure(0, weight=0)  # controls fixed
        self.root.grid_rowconfigure(1, weight=1)  # output flex
        self.root.grid_rowconfigure(2, weight=0)  # log collapsible
        self.root.grid_columnconfigure(0, weight=1)

        # Restore window size/state
        win_geom = self.config.get("window_size", "1200x950+100+100")
        self.root.geometry(win_geom)

        win_state = self.config.get("window_state", "normal")
        if win_state != "normal":
            self.root.after_idle(lambda: self.root.state(win_state))

        # ✅ Restore collapsible states
        states = self.config.get("collapsible_states", {})
        self.filter_date_section.set_state(states.get("filter_date", "expanded"))
        self.table_section.set_state(states.get("table", "expanded"))
        self.log_section.set_state(states.get("log", "expanded"))

        # Protocols/bindings
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<KeyPress>", self.on_key_press)
        self.root.bind("<KeyRelease>", self.on_key_release)

        # Show window
        self.root.after(50, self.root.deiconify)

    # -------------------------------
    # Key handling
    # -------------------------------
    def on_key_press(self, event):
        key = event.keysym
        self.held_keys.add(key)

        if key in ("Shift_L", "Shift_R"):
            self.modifiers.add("Shift")
            self.reprocess_held_keys()
        elif key in ("Control_L", "Control_R"):
            self.modifiers.add("Control")
            self.reprocess_held_keys()
        elif key in ("Alt_L", "Alt_R"):
            self.modifiers.add("Alt")
            self.reprocess_held_keys()
        else:
            self.fire_bind(key)

    def on_key_release(self, event):
        key = event.keysym
        self.held_keys.discard(key)

        if key in ("Shift_L", "Shift_R"):
            self.modifiers.discard("Shift")
            self.reprocess_held_keys()
        elif key in ("Control_L", "Control_R"):
            self.modifiers.discard("Control")
            self.reprocess_held_keys()
        elif key in ("Alt_L", "Alt_R"):
            self.modifiers.discard("Alt")
            self.reprocess_held_keys()

    def reprocess_held_keys(self):
        for key in list(self.held_keys):
            if key not in ("Shift_L", "Shift_R", "Control_L", "Control_R", "Alt_L", "Alt_R"):
                self.fire_bind(key)

    def fire_bind(self, key):
        combo = []
        if "Shift" in self.modifiers:
            combo.append("Shift")
        if "Control" in self.modifiers:
            combo.append("Control")
        if "Alt" in self.modifiers:
            combo.append("Alt")
        combo.append(key)

    # -------------------------------
    # UI building
    # -------------------------------
    def build_controls(self):
        # --- Filter, Date & Columns collapsible ---
        self.control_frame = ctk.CTkFrame(self.output_frame, corner_radius=12)
        self.control_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)

        self.filter_date_section = CollapsibleSection(
            self.control_frame, title="Filter, Date & Columns"
        )
        self.filter_date_section.pack(fill="x", padx=10, pady=5)

        # Row container for filter/date/columns
        row_frame = ctk.CTkFrame(self.filter_date_section.content, fg_color="transparent")
        row_frame.pack(fill="x", padx=5, pady=5)

        # ---------------- Filter box ----------------
        filter_box = ctk.CTkFrame(row_frame, corner_radius=8)
        filter_box.pack(side="left", padx=5, pady=5, anchor="n")

        ctk.CTkLabel(filter_box, text="Filter Type:").pack(anchor="w", padx=5, pady=2)
        self.filter_type = ctk.StringVar(value=self.config.get("filter_type", "device_name"))
        self.filter_menu = ctk.CTkComboBox(
            filter_box, variable=self.filter_type,
            values=["device_name", "user_id"], width=180
        )
        self.filter_menu.pack(anchor="w", padx=5, pady=2)

        ctk.CTkLabel(filter_box, text="Filter Value:").pack(anchor="w", padx=5, pady=2)
        self.filter_value = ctk.CTkEntry(filter_box, width=200, placeholder_text="Enter value")
        self.filter_value.insert(0, self.config.get("filter_value", ""))
        self.filter_value.pack(anchor="w", padx=5, pady=2)

        # ---------------- Date box ----------------
        date_box = ctk.CTkFrame(row_frame, corner_radius=8)
        date_box.pack(side="left", padx=5, pady=5, anchor="n")

        ctk.CTkLabel(date_box, text="Start Date:").pack(anchor="w", padx=5, pady=2)
        self.start_date_entry = ctk.CTkEntry(date_box, width=180)
        self.start_date_entry.insert(0, self.config.get(
            "start_date", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        ))
        self.start_date_entry.pack(anchor="w", padx=5, pady=2)

        ctk.CTkLabel(date_box, text="End Date:").pack(anchor="w", padx=5, pady=2)
        self.end_date_entry = ctk.CTkEntry(date_box, width=180)
        self.end_date_entry.insert(0, self.config.get(
            "end_date", datetime.now().strftime("%Y-%m-%d")
        ))
        self.end_date_entry.pack(anchor="w", padx=5, pady=2)

        # ---------------- Columns box ----------------
        col_frame = ctk.CTkFrame(row_frame, corner_radius=8)
        col_frame.pack(side="left", fill="x", expand=True, padx=5, pady=5, anchor="n")

        # Available columns come from color_map at startup
        self.available_columns = list(self.color_map.keys())
        self.col_vars = {}
        selected_cols = self.config.get("columns", ["fan_tach_rpm", "coolant_temp_f", "ebox_temp_f"])

        rows = 4  # number of rows for checkbox layout
        for i, col in enumerate(self.available_columns):
            var = ctk.BooleanVar(value=col in selected_cols)
            chk = ctk.CTkCheckBox(
                col_frame,
                text=col,
                variable=var,
                command=self.on_column_change,
                fg_color=self.color_map[col],
                text_color=self.color_map[col]
            )
            chk.grid(
                row=i % rows,
                column=i // rows,
                padx=5, pady=5, sticky="w"
            )
            self.col_vars[col] = var

    def build_output(self):
        self.output_frame = ctk.CTkFrame(self.root, corner_radius=12)
        self.output_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)

        # Configure grid: 4 rows, 1 col
        self.output_frame.grid_rowconfigure(0, weight=1)  # plot flexes
        self.output_frame.grid_rowconfigure(1, weight=0)  # table collapsible
        self.output_frame.grid_rowconfigure(2, weight=0)  # filter/date/columns
        self.output_frame.grid_rowconfigure(3, weight=0)  # run query controls
        self.output_frame.grid_columnconfigure(0, weight=1)

        # ---------------- Plot ----------------
        self.plot_frame = ctk.CTkFrame(self.output_frame, corner_radius=12)
        self.plot_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        self.plot_manager = PlotManager(
            self.plot_frame,
            on_select=self.on_select,
            on_key=self.on_key
        )

        # ---------------- Data Table ----------------
        self.table_section = CollapsibleSection(self.output_frame, title="Data Table")
        self.table_section.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)

        self.table_frame = ctk.CTkFrame(self.table_section.content, corner_radius=12)
        self.table_frame.pack(fill="both", expand=True)

        # ---------------- Filter/Date/Columns ----------------
        # built in build_controls()
        # placed in row=2 by that function

        # ---------------- Run Query Controls ----------------
        self.run_frame = ctk.CTkFrame(self.output_frame, fg_color="transparent")
        self.run_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=10)

        self.run_btn = ctk.CTkButton(
            self.run_frame, text="▶ Run Query",
            command=self.on_run, fg_color="#1f6aa5", hover_color="#144870"
        )
        self.run_btn.pack(side="left", padx=10)

        self.timer_label = ctk.CTkLabel(self.run_frame, text="")
        self.timer_label.pack(side="left", padx=10)

        self.status_label = ctk.CTkLabel(
            self.run_frame, text="Device: N/A | User: N/A | Step: Minute"
        )
        self.status_label.pack(side="left", padx=10)

    def build_log(self):
        # Log collapsible goes in root row=2
        self.log_section = CollapsibleSection(self.root, title="Process Log")
        self.log_section.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

        self.log_section.content.grid_rowconfigure(0, weight=1)
        self.log_section.content.grid_columnconfigure(0, weight=1)

        self.log_text = ctk.CTkTextbox(self.log_section.content, height=100)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # -------------------------------
    # Table handling
    # -------------------------------
    def show_table(self, df):
        if df is None or df.empty:
            for widget in self.table_frame.winfo_children():
                widget.destroy()
            ctk.CTkLabel(self.table_frame, text="No data available").pack()
            return

        if not hasattr(self, "sheet"):
            self.sheet = Sheet(
                self.table_frame,
                data=df.values.tolist(),
                headers=list(df.columns),
                height=200,
            )
            self.sheet.enable_bindings((
                "single_select", "row_select", "column_select", "drag_select",
                "arrowkeys", "copy", "select_all"
            ))
            self.sheet.pack(fill="both", expand=True)
        else:
            self.sheet.set_sheet_data(df.values.tolist())
            self.sheet.headers(df.columns.tolist())

    # -------------------------------
    # Cache + log + timer helpers (unchanged from before)
    # -------------------------------
    def restore_cached_plot(self):
        df, columns = self.plot_manager.load_cache()
        if df is not None and not df.empty:
            self.df = df
            self.plot_manager.plot_data(df, self.get_selected_columns(), fresh=True, color_map=self.color_map)
            self.show_table(df)
            self.log("📂 Restored last cached plot.")

            device_val = df["device_name"].dropna().iloc[0] if "device_name" in df else "?"
            user_val = df["user_id"].dropna().iloc[0] if "user_id" in df else "?"
            self.status_label.configure(text=f"Device: {device_val} | User: {user_val} | Step: Minute")

    def log(self, message: str):
        if self.is_closing:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")
        try:
            self.log_text.insert("end", f"[{timestamp}] {message}\n")
            self.log_text.see("end")
            self.root.update_idletasks()
        except Exception:
            pass

    def start_timer(self):
        self.query_start_time = time.time()
        self.timer_running = True
        self.update_timer()

    def stop_timer(self):
        self.timer_running = False
        self.cancel_afters()

    def update_timer(self):
        if self.is_closing or not self.timer_running:
            return
        elapsed = time.time() - self.query_start_time
        self.timer_label.configure(text=f"⏱ Elapsed: {elapsed:.1f}s")
        if not self.is_closing:
            self.safe_after(100, self.update_timer)

    def safe_after(self, delay, func, *args, **kwargs):
        if self.is_closing:
            return
        try:
            after_id = self.root.after(delay, func, *args, **kwargs)
            self.after_ids.append(after_id)
            return after_id
        except Exception as e:
            self.log(f"[AFTER] Failed to schedule: {e}")
            return

    def cancel_afters(self):
        for aid in self.after_ids:
            try:
                self.root.after_cancel(aid)
            except Exception as e:
                self.log(f"[AFTER] Failed to cancel id={aid}: {e}")
        self.after_ids.clear()
        try:
            jobs = self.root.tk.call("after", "info")
            for job in jobs:
                try:
                    self.root.after_cancel(job)
                except Exception:
                    pass
        except Exception:
            pass

    def get_selected_columns(self):
        return [col for col, var in self.col_vars.items() if var.get()]

    # -------------------------------
    # Events
    # -------------------------------
    def on_run(self):
        if not self.filter_value.get().strip():
            messagebox.showerror("Error", "Filter Value cannot be empty.")
            return
        self.start_timer()
        t = threading.Thread(target=self._run_query_worker, daemon=True)
        self.threads.append(t)
        t.start()

    def _run_query_worker(self):
        try:
            df = self.query_manager.run_query(
                filter_type=self.filter_type.get(),
                filter_value=self.filter_value.get(),
                start_date_str=self.start_date_entry.get(),
                end_date_str=self.end_date_entry.get(),
                selected_columns=self.get_selected_columns()
            )
            self.df = df
            if "updated_at" not in df.columns:
                raise ValueError("Expected column 'updated_at' not found in query results.")

            if df.empty:
                self.log("⚠️ Query returned no results for the given filter/date range.")
            else:
                self.log(f"✅ Query complete. Retrieved {len(df)} rows.")

            self.safe_after(0, self.plot_manager.plot_data,
                            df, self.get_selected_columns(), True, self.color_map)
            self.safe_after(0, self.show_table, df)

            if not df.empty:
                device_val = df["device_name"].dropna().iloc[0] if "device_name" in df else self.filter_value.get()
                user_val = df["user_id"].dropna().iloc[0] if "user_id" in df else "?"
                self.safe_after(
                    0,
                    self.status_label.configure,
                    {"text": f"Device: {device_val} | User: {user_val} | Step: Minute"}
                )

        except Exception as e:
            if not self.is_closing:
                self.log(f"❌ Error: {e}")
                self.safe_after(0, messagebox.showerror, "Error", str(e))

        finally:
            self.stop_timer()

    def on_column_change(self):
        if self.df is not None:
            self.plot_manager.plot_data(
                self.df, self.get_selected_columns(), fresh=False, color_map=self.color_map
            )
            self.show_table(self.df)

    def on_select(self, eclick, erelease):
        pass

    def on_key(self, event):
        pass

    def on_close(self):
        if self.is_closing:
            return
        self.is_closing = True
        self.log("[CLOSE] Starting shutdown sequence")

        try:
            state = self.root.state()
            cfg = {
                "filter_type": self.filter_type.get(),
                "filter_value": self.filter_value.get(),
                "start_date": self.start_date_entry.get(),
                "end_date": self.end_date_entry.get(),
                "columns": self.get_selected_columns(),
                "window_state": state,
            }

            if state == "normal":
                cfg["window_size"] = self.root.geometry()

            # ✅ Save collapsible states
            cfg["collapsible_states"] = {
                "filter_date": self.filter_date_section.get_state(),
                "table": self.table_section.get_state(),
                "log": self.log_section.get_state(),
            }

            config_manager.save_config(cfg)
            self.log("[CLOSE] Saved config")
        except Exception as e:
            self.log(f"[CLOSE] Failed to save config: {e}")

        self.cancel_afters()

        for t in self.threads:
            if t.is_alive():
                self.log(f"[CLOSE] Thread {t.name} still alive, skipping join")

        try:
            self.root.quit()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    def run(self):
        self.root.mainloop()
