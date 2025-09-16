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

        self.plot_manager = None
        self.threads = []
        self.after_ids = []
        self.is_closing = False
        self.query_start_time = None
        self.timer_running = False
        self.table_dragging = False
        self.table_drag_start = None
        self.table_panning = False
        self.table_pan_start = None
        self._row_cache = []  # full data as list-of-lists (all columns)
        self._table_order = []  # current row order (list of row indices)
        self._sorting = False  # debounce flag
        self._sort_dir = {}  # per-column toggle (True=asc, False=desc)
        self._last_cell = (0, 0)
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
        self.query_manager = QueryManager(logger=self.log, root=self.root)
        # Load config
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

        # Restore collapsible states
        states = self.config.get("collapsible_states", {})
        self.filter_date_section.set_state(states.get("filter_date", "expanded"))
        self.table_section.set_state(states.get("table", "expanded"))
        self.log_section.set_state(states.get("log", "expanded"))

        # Protocols/bindings
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<KeyPress>", self.on_key_press)
        self.root.bind("<KeyRelease>", self.on_key_release)
        # Bind mouse events for navigation


        # Show window
        self.root.after(50, self.root.deiconify)

    # -------------------------------
    # Key handling
    # -------------------------------
    def _get_current_cell(self):
        # Try the most specific first
        try:
            # tksheet >= 7 returns list of (r,c)
            cells = self.sheet.get_selected_cells()
            if cells:
                r, c = int(cells[0][0]), int(cells[0][1])
                self._last_cell = (r, c)
                return r, c
        except Exception:
            pass

        try:
            cur = self.sheet.get_currently_selected()
            if isinstance(cur, tuple) and len(cur) >= 2:
                r, c = int(cur[0]), int(cur[1])
                self._last_cell = (r, c)
                return r, c
        except Exception:
            pass

        # Fallbacks when row/column selection is active
        try:
            rows = self.sheet.get_selected_rows()
            if rows:
                r = int(rows[0])
                c = self._last_cell[1]
                self._last_cell = (r, c)
                return r, c
        except Exception:
            pass

        try:
            cols = self.sheet.get_selected_columns()
            if cols:
                c = int(cols[0])
                r = self._last_cell[0]
                self._last_cell = (r, c)
                return r, c
        except Exception:
            pass

        return self._last_cell

    def _set_focus(self, r, c):
        # clamp to data shape
        nrows, ncols = self._get_shape()
        r = max(0, min(r, max(0, nrows - 1)))
        c = max(0, min(c, max(0, ncols - 1)))

        # prefer select_cell (it updates selection + caret)
        try:
            self.sheet.select_cell(r, c, keep_other_selections=False)
        except Exception:
            try:
                self.sheet.set_currently_selected(r, c)
            except Exception:
                pass

        # make it visible and ensure keyboard focus is on a tksheet widget
        try:
            self.sheet.see(r, c)
        except Exception:
            pass
        try:
            self.sheet.focus_set()
        except Exception:
            pass

        self._last_cell = (r, c)
        self._force_redraw()

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
        self.col_frame = ctk.CTkFrame(row_frame, corner_radius=8)
        self.col_frame.pack(side="left", fill="x", expand=True, padx=5, pady=5)

        # Two frames for metrics vs others
        self.metrics_col_frame = ctk.CTkFrame(self.col_frame, corner_radius=8)
        self.metrics_col_frame.pack(side="left", fill="y", padx=5, pady=5)

        self.other_col_frame = ctk.CTkFrame(self.col_frame, corner_radius=8)
        self.other_col_frame.pack(side="left", fill="y", padx=5, pady=5)

        # Placeholder dict for column checkboxes (rebuilt in show_table)
        self.col_vars = {}

        # Master toggles for later
        self.metrics_toggle = ctk.BooleanVar(value=True)
        self.other_toggle = ctk.BooleanVar(value=True)

    def toggle_metrics(self):
        state = self.metrics_toggle.get()
        for col, var in self.col_vars.items():
            if col in self.color_map:  # metrics only
                var.set(state)
        self.on_column_change()

    def toggle_others(self):
        state = self.other_toggle.get()
        for col, var in self.col_vars.items():
            if col not in self.color_map:  # others only
                var.set(state)
        self.on_column_change()

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
    def _to_canvas_xy(self, canvas, event):
        # translate event coords (from the Sheet widget) to target canvas coords
        return (event.x_root - canvas.winfo_rootx(),
                event.y_root - canvas.winfo_rooty())

    def _sync_headers(self):
        # keep column header (CH) and row index (RI) aligned with the main table (MT)
        try:
            x0, _ = self.sheet.MT.xview()
            self.sheet.CH.xview_moveto(x0)
        except Exception:
            pass
        try:
            y0, _ = self.sheet.MT.yview()
            self.sheet.RI.yview_moveto(y0)
        except Exception:
            pass

    def _clamp_view(self):
        # don’t allow overscroll (prevents “white” edges)
        MT = self.sheet.MT
        x0, x1 = MT.xview()
        y0, y1 = MT.yview()
        win_w = max(1e-9, x1 - x0)
        win_h = max(1e-9, y1 - y0)

        if x0 < 0.0:
            MT.xview_moveto(0.0)
        elif x1 > 1.0:
            MT.xview_moveto(max(0.0, 1.0 - win_w))

        if y0 < 0.0:
            MT.yview_moveto(0.0)
        elif y1 > 1.0:
            MT.yview_moveto(max(0.0, 1.0 - win_h))

    def _force_redraw(self):
        # Coalesce multiple requests into one after_idle call
        if getattr(self, "_frame_scheduled", False):
            return
        self._frame_scheduled = True

        def _do():
            try:
                self.sheet.refresh()
            except Exception:
                pass
            self._frame_scheduled = False

        self.root.after_idle(_do)

    def show_table(self, df, col_states=None):
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
            self._bind_sheet_nav_keys()
            self.sheet.enable_bindings((
                "single_select", "row_select", "column_select", "drag_select", "arrowkeys",
                "column_width_resize", "row_height_resize", "double_click_column_resize",
                "copy", "select_all"
            ))
            self._sort_dir = {}  # e.g. {"updated_at": True}  True=ascending, False=descending
            self.sheet.pack(fill="both", expand=True)
            # ensure the widget grabs focus on click and initially

            self.sheet.focus_set()
            # Right-click drag bindings for table panning
            self.sheet.bind("<Button-3>", self.on_table_pan_start)
            self.sheet.bind("<B3-Motion>", self.on_table_pan_drag)
            self.sheet.bind("<ButtonRelease-3>", self.on_table_pan_stop)
            self.sheet.bind("<Double-1>", self._hdr_double_click, add="+")
            self.sheet.bind("<Button-1>", lambda e: self.sheet.focus_set(), add="+")
            self.sheet.focus_set()

            # cache all rows once (object keeps mixed types intact)
            self._row_cache = self.df.astype(object).values.tolist()
            self._table_order = list(range(len(self._row_cache)))  # identity order


        else:
            self.sheet.set_sheet_data(df.values.tolist())
            self.sheet.headers(df.columns.tolist())
            # cache all rows once (object keeps mixed types intact)
            self._row_cache = self.df.astype(object).values.tolist()
            self._table_order = list(range(len(self._row_cache)))  # identity order

        # 🔄 Build checkboxes with states
        self.build_column_checkboxes(df.columns, col_states)

        # Update visible columns according to checkboxes
        self.update_table_columns()

    def _get_col_widths(self):
        # derive widths from current col_positions (most robust across tksheet versions)
        try:
            pos = list(self.sheet.MT.col_positions)
            return [pos[i + 1] - pos[i] for i in range(len(pos) - 1)]
        except Exception:
            # fallback to per-column getter if available
            widths = []
            try:
                n = len(self.sheet.headers())
            except Exception:
                n = 0
            for i in range(n):
                try:
                    w = self.sheet.column_width(i)  # some versions support getter
                except Exception:
                    w = None
                widths.append(w)
            return widths

    def _set_col_widths(self, widths):
        # apply as many as we have columns for
        for i, w in enumerate(widths):
            if w is None:
                continue
            try:
                # most versions support setter like this
                self.sheet.column_width(i, w, redraw=False)
            except TypeError:
                try:
                    self.sheet.column_width(i, w)
                except Exception:
                    pass
        try:
            self.sheet.refresh()
        except Exception:
            pass

    def _hdr_double_click(self, event):
        # Only sort when double-clicking inside the header,
        # and NOT near a divider (so auto-resize still wins there).
        try:
            if self.sheet.identify_region(event) != "header":
                return
        except Exception:
            return

        if self._is_near_header_divider(event):
            # Let tksheet's "double_click_column_resize" do its thing
            return

        if self._is_near_header_divider(event):
            return
        # Reuse your sorter (decides column & toggles direction)
        self._maybe_sort_by_header(event)

    def _is_near_header_divider(self, event, eps=6):
        """Return True if pointer is within `eps` px of a column divider in the header."""
        try:
            # x in ColumnHeader *widget* coords
            ch = self.sheet.CH
            local_x = event.x_root - ch.winfo_rootx()
            # convert to canvas coords (accounts for horizontal scroll)
            cx = ch.canvasx(local_x)

            # Prefer CH.col_positions; fall back to MT.col_positions
            positions = getattr(ch, "col_positions", None)
            if not positions:
                positions = getattr(self.sheet.MT, "col_positions", None)
            if not positions:
                return False

            # Close to any divider?
            for p in positions:
                if abs(cx - p) <= eps:
                    return True
        except Exception:
            pass
        return False

    def _bind_sheet_nav_keys(self):
        w = self.sheet
        # Ctrl + arrows
        w.bind("<Control-Left>", self._nav_left_end)
        w.bind("<Control-Right>", self._nav_right_end)
        w.bind("<Control-Up>", self._nav_top_end)
        w.bind("<Control-Down>", self._nav_bottom_end)

        # (nice extras)
        w.bind("<Home>", self._nav_left_end)
        w.bind("<End>", self._nav_right_end)
        w.bind("<Control-Home>", self._nav_top_left)
        w.bind("<Control-End>", self._nav_bottom_right)

    def _sync_headers(self):
        """Keep column header (CH) and row index (RI) aligned with the main table (MT)."""
        try:
            x0, _ = self.sheet.MT.xview()
            self.sheet.CH.xview_moveto(x0)
        except Exception:
            pass
        try:
            y0, _ = self.sheet.MT.yview()
            self.sheet.RI.yview_moveto(y0)
        except Exception:
            pass

    def _goto_x(self, frac: float):
        MT = self.sheet.MT
        x0, x1 = MT.xview()
        win_w = max(1e-9, x1 - x0)
        left = max(0.0, min(1.0 - win_w, frac))
        MT.xview_moveto(left)
        self._sync_headers()
        self._force_redraw()  # <-- important

    def _goto_y(self, frac: float):
        MT = self.sheet.MT
        y0, y1 = MT.yview()
        win_h = max(1e-9, y1 - y0)
        top = max(0.0, min(1.0 - win_h, frac))
        MT.yview_moveto(top)
        self._sync_headers()
        self._force_redraw()  # <-- important

    def _get_shape(self):
        # rows = current order (displayed), cols = visible columns only
        try:
            nrows = len(self._table_order) if self._table_order else (len(self.df) if self.df is not None else 0)
            if self.df is not None:
                visible_cols = [i for i, col in enumerate(self.df.columns) if self.col_vars[col].get()]
                ncols = len(visible_cols)
            else:
                ncols = 0
        except Exception:
            nrows, ncols = 0, 0
        return nrows, ncols

    def _get_current_cell(self):
        try:
            cur = self.sheet.get_currently_selected()
            if isinstance(cur, tuple) and len(cur) >= 2:
                return int(cur[0]), int(cur[1])
        except Exception:
            pass
        return 0, 0

    def _set_focus(self, r, c):
        r = max(0, r);
        c = max(0, c)
        try:
            self.sheet.set_currently_selected(r, c)
        except Exception:
            pass
        try:
            # if available in your tksheet version
            self.sheet.see(r, c)
        except Exception:
            pass
        self._force_redraw()

    # ---------- NAV + VIEW HELPERS (add to class) ----------
    def _visible_shape(self):
        """Rows/cols for the currently visible sheet data."""
        try:
            data = self.sheet.get_sheet_data(return_copy=False)
            nrows = len(data)
            ncols = len(data[0]) if data else 0
            return nrows, ncols
        except Exception:
            return 0, 0

    def _get_current_cell(self):
        try:
            cur = self.sheet.get_currently_selected()
            if isinstance(cur, tuple) and len(cur) >= 2:
                # protect against None
                r = int(cur[0]) if cur[0] is not None else 0
                c = int(cur[1]) if cur[1] is not None else 0
                return r, c
        except Exception:
            pass
        return 0, 0

    def _set_focus(self, r, c):
        r = max(0, r);
        c = max(0, c)
        try:
            self.sheet.set_currently_selected(r, c)
            # if available in your tksheet version:
            try:
                self.sheet.see(r, c)
            except Exception:
                pass
        except Exception:
            pass
        self._force_redraw()

    def _goto_x(self, frac: float):
        MT = self.sheet.MT
        x0, x1 = MT.xview()
        win_w = max(1e-9, x1 - x0)
        left = max(0.0, min(1.0 - win_w, frac))
        MT.xview_moveto(left)
        self._sync_headers()
        self._force_redraw()

    def _goto_y(self, frac: float):
        MT = self.sheet.MT
        y0, y1 = MT.yview()
        win_h = max(1e-9, y1 - y0)
        top = max(0.0, min(1.0 - win_h, frac))
        MT.yview_moveto(top)
        self._sync_headers()
        self._force_redraw()

    # ----- Ctrl+Arrow / Home/End handlers (called by _bind_sheet_nav_keys) -----
    def _nav_left_end(self, event=None):
        self._goto_x(0.0)
        r, _ = self._get_current_cell()
        self._set_focus(r, 0)
        return "break"

    def _nav_right_end(self, event=None):
        # only scroll if there is room to scroll
        try:
            x0, x1 = self.sheet.MT.xview()
            if (x1 - x0) < 0.999:  # there is horizontal scrollable space
                self._goto_x(1.0)
        except Exception:
            pass

        r, _ = self._get_current_cell()
        _, ncols = self._get_shape()  # <-- now visible column count
        self._set_focus(r, max(0, ncols - 1))  # last visible column
        return "break"

    def _nav_top_end(self, event=None):
        self._goto_y(0.0)
        _, c = self._get_current_cell()
        self._set_focus(0, c)
        return "break"

    def _nav_bottom_end(self, event=None):
        self._goto_y(1.0)
        _, c = self._get_current_cell()
        nrows, _ = self._get_shape()
        self._set_focus(max(0, nrows - 1), c)
        return "break"

    # (same pattern for the other three)

    def _nav_top_left(self, event=None):
        self._goto_y(0.0);
        self._goto_x(0.0)
        self._set_focus(0, 0)
        return "break"

    def _nav_bottom_right(self, event=None):
        self._goto_y(1.0);
        self._goto_x(1.0)
        nrows, ncols = self._visible_shape()
        self._set_focus(max(0, nrows - 1), max(0, ncols - 1))
        return "break"

    def build_column_checkboxes(self, columns, col_states=None):
        # Clear old
        for widget in self.metrics_col_frame.winfo_children():
            widget.destroy()
        for widget in self.other_col_frame.winfo_children():
            widget.destroy()

        self.col_vars = {}
        max_rows = 4

        metrics = [col for col in columns if col in self.color_map]
        others = [col for col in columns if col not in self.color_map]

        # --- helpers ---
        def update_select_all_states():
            # Metrics group
            if metrics:
                self.metrics_toggle.set(all(self.col_vars[c].get() for c in metrics))
            else:
                self.metrics_toggle.set(False)
            # Others group
            if others:
                self.other_toggle.set(all(self.col_vars[c].get() for c in others))
            else:
                self.other_toggle.set(False)

        def per_box_cmd():
            # Called when any individual checkbox is toggled
            self.on_column_change()
            update_select_all_states()

        # --- Metrics checkboxes ---
        for i, col in enumerate(metrics):
            default_val = col_states.get(col, True) if col_states else True
            var = ctk.BooleanVar(value=default_val)
            chk = ctk.CTkCheckBox(
                self.metrics_col_frame,
                text=col,
                variable=var,
                command=per_box_cmd,
                fg_color=self.color_map[col],
                text_color=self.color_map[col],
            )
            chk.grid(row=i % max_rows, column=i // max_rows, padx=5, pady=5, sticky="w")
            self.col_vars[col] = var

        # Select All (metrics)
        if metrics:
            # initialize based on current states
            self.metrics_toggle.set(all(self.col_vars[c].get() for c in metrics))
            row = len(metrics) % max_rows
            col = len(metrics) // max_rows
            ctk.CTkCheckBox(
                self.metrics_col_frame,
                text="Select All",
                variable=self.metrics_toggle,
                command=lambda: (self.toggle_metrics(), update_select_all_states()),
            ).grid(row=row, column=col, padx=5, pady=5, sticky="w")

        # --- Other checkboxes ---
        for i, col in enumerate(others):
            default_val = col_states.get(col, True) if col_states else True
            var = ctk.BooleanVar(value=default_val)
            chk = ctk.CTkCheckBox(
                self.other_col_frame,
                text=col,
                variable=var,
                command=per_box_cmd,
            )
            chk.grid(row=i % max_rows, column=i // max_rows, padx=5, pady=5, sticky="w")
            self.col_vars[col] = var

        # Select All (others)
        if others:
            self.other_toggle.set(all(self.col_vars[c].get() for c in others))
            row = len(others) % max_rows
            col = len(others) // max_rows
            ctk.CTkCheckBox(
                self.other_col_frame,
                text="Select All",
                variable=self.other_toggle,
                command=lambda: (self.toggle_others(), update_select_all_states()),
            ).grid(row=row, column=col, padx=5, pady=5, sticky="w")

    # -------------------------------
    # Cache + log + timer helpers (unchanged from before)
    # -------------------------------
    def restore_cached_plot(self):
        df, columns, col_states = self.plot_manager.load_cache()
        if df is not None and not df.empty:
            self.df = df

            # plot only saved metrics
            self.plot_manager.plot_data(df, columns, fresh=True, color_map=self.color_map)

            # pass saved states into show_table
            self.show_table(df, col_states)

            self.on_column_change()  # apply to plot

            self.log("📂 Restored last cached plot.")

            device_val = df["device_name"].dropna().iloc[0] if "device_name" in df else "?"
            user_val = df["user_id"].dropna().iloc[0] if "user_id" in df else "?"
            self.status_label.configure(
                text=f"Device: {device_val} | User: {user_val}"
            )

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

    def _maybe_sort_by_header(self, event):
        # Only header clicks
        try:
            if self.sheet.identify_region(event) != "header":
                return
            vis_col = self.sheet.identify_column(event, exclude_header=False)
        except Exception:
            return
        if vis_col is None or self.df is None or self.df.empty:
            return

        visible_cols = [i for i, col in enumerate(self.df.columns) if self.col_vars[col].get()]
        if not (0 <= vis_col < len(visible_cols)):
            return
        col_idx = visible_cols[vis_col]
        col_name = self.df.columns[col_idx]

        # toggle direction per column
        asc = self._sort_dir.get(col_name, True)
        self._sort_dir[col_name] = not asc

        # remember scroll to avoid white flash/jumps
        MT = self.sheet.MT
        try:
            x0, _ = MT.xview()
            y0, _ = MT.yview()
        except Exception:
            x0 = y0 = 0.0

        def worker():
            def keynorm(v):
                if v is None:
                    return (1, 0, "")
                if isinstance(v, (int, float)):
                    return (0, 0, v)
                return (0, 1, str(v).lower())

            try:
                new_order = sorted(
                    self._table_order,
                    key=lambda i: keynorm(self._row_cache[i][col_idx]),
                    reverse=not asc,
                )
            except Exception:
                # defensive fallback
                new_order = sorted(
                    self._table_order,
                    key=lambda i: (self._row_cache[i][col_idx] is None,
                                   str(self._row_cache[i][col_idx]).lower()),
                    reverse=not asc,
                )

            def apply():
                self._apply_row_order(new_order)
                try:
                    MT.xview_moveto(x0);
                    MT.yview_moveto(y0)
                    self._sync_headers()
                except Exception:
                    pass
                self.log(f'Sorted by “{col_name}” ({"asc" if asc else "desc"})')

            self.safe_after(0, apply)

        threading.Thread(target=worker, daemon=True).start()

    def get_selected_metrics(self):
        """Columns that are both checked and in color_map (for plotting + tooltips)."""
        return [col for col, var in self.col_vars.items() if var.get() and col in self.color_map]

    def get_selected_table_columns(self):
        """Columns that are checked, regardless of whether they're metrics or other."""
        return [col for col, var in self.col_vars.items() if var.get()]

    def _to_canvas_xy(self, canvas, event):
        # translate event coords (from self.sheet) to the target canvas' coords
        return (event.x_root - canvas.winfo_rootx(),
                event.y_root - canvas.winfo_rooty())

    def on_table_pan_start(self, event):
        self.table_panning = True
        mx, my = self._to_canvas_xy(self.sheet.MT, event)
        self.sheet.MT.scan_mark(mx, my)

    def on_table_pan_drag(self, event):
        if not self.table_panning:
            return
        MT = self.sheet.MT
        mx, my = self._to_canvas_xy(MT, event)

        # edge guards (keep yours if you like)
        x0, x1 = MT.xview()
        y0, y1 = MT.yview()
        prev = getattr(self, "_last_scan_xy", (mx, my))
        dx, dy = (mx - prev[0], my - prev[1])
        self._last_scan_xy = (mx, my)
        if (x0 <= 1e-6 and dx > 0) or (y0 <= 1e-6 and dy > 0):
            return

        MT.scan_dragto(mx, my, gain=1)
        self._sync_headers()
        # Don't clamp + repaint twice; let a single coalesced refresh run:
        self._force_redraw()

    def on_table_pan_stop(self, event):
        self.table_panning = False

    def _pan_refresh_tick(self):
        if not getattr(self, "table_panning", False):
            return
        try:
            self.sheet.refresh()
        except Exception:
            pass
        self._pan_refresh_job = self.root.after(33, self._pan_refresh_tick)

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

    def _update_select_all_checks(self):
        # Metrics group
        metrics_cols = [c for c in self.col_vars if c in self.color_map]
        if metrics_cols:
            all_metrics_checked = all(self.col_vars[c].get() for c in metrics_cols)
            # If you uncheck any metric, this will flip "Select All" off
            self.metrics_toggle.set(all_metrics_checked)
        else:
            self.metrics_toggle.set(False)

        # Others group
        other_cols = [c for c in self.col_vars if c not in self.color_map]
        if other_cols:
            all_others_checked = all(self.col_vars[c].get() for c in other_cols)
            self.other_toggle.set(all_others_checked)
        else:
            self.other_toggle.set(False)

    def _run_query_worker(self):
        try:
            if not self.query_manager.connect():
                self.log("⏹ Query aborted (no DB connection).")
                return  # stop cleanly if cancelled
            # If no checkboxes yet (first run / no cache), request ALL columns
            sel_cols = self.get_selected_table_columns()
            if not sel_cols:  # <- key line
                sel_cols = None  # None = ask QueryManager for all columns

            df = self.query_manager.run_query(
                filter_type=self.filter_type.get(),
                filter_value=self.filter_value.get(),
                start_date_str=self.start_date_entry.get(),
                end_date_str=self.end_date_entry.get(),
                selected_columns=sel_cols  # pass None or full list
            )

            self.df = df
            if "updated_at" not in df.columns:
                raise ValueError("Expected column 'updated_at' not found in query results.")

            if df.empty:
                self.log("⚠️ Query returned no results for the given filter/date range.")
            else:
                self.log(f"Query complete. Retrieved {len(df)} rows.")

            def _render_first_time():
                # 1) build the table so checkboxes exist
                self.show_table(df, None)
                # 2) get current metric selection; if none yet, let PlotManager auto-pick
                metrics = self.get_selected_metrics()
                sel = metrics if metrics else None
                self.plot_manager.plot_data(df, sel, True, self.color_map)

            self.safe_after(0, _render_first_time)

            if not df.empty:
                device_val = df["device_name"].dropna().iloc[0] if "device_name" in df else self.filter_value.get()
                user_val = df["user_id"].dropna().iloc[0] if "user_id" in df else "?"
                self.safe_after(
                    0,
                    self.status_label.configure,
                    {"text": f"Device: {device_val} | User: {user_val} | Step: Minute"}
                )
            # Save cache with full checkbox states
            self.plot_manager._save_cache(
                df,
                self.get_selected_metrics(),  # metrics that were plotted
                {col: var.get() for col, var in self.col_vars.items()}  # full states
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
                self.df, self.get_selected_metrics(), fresh=False, color_map=self.color_map
            )
            self.update_table_columns()

            # keep "Select All" boxes in sync with individual boxes
            self._update_select_all_checks()

            self.plot_manager._save_cache(
                self.df,
                self.get_selected_metrics(),
                {col: var.get() for col, var in self.col_vars.items()}
            )

    def update_table_columns(self):
        if not hasattr(self, "sheet") or self.df is None:
            return

        MT = self.sheet.MT
        try:
            x0, _ = MT.xview()
            y0, _ = MT.yview()
        except Exception:
            x0 = y0 = 0.0

        # Snapshot widths only if the visible column count stays the same
        try:
            old_w = self._get_col_widths()
            old_count = len(old_w)
        except Exception:
            old_w = None
            old_count = None

        visible_cols = [i for i, col in enumerate(self.df.columns) if self.col_vars[col].get()]
        ordered_rows = [self._row_cache[i] for i in self._table_order]
        visible_data = [[row[j] for j in visible_cols] for row in ordered_rows]

        # Update without intermediate redraws
        try:
            # If your tksheet version supports redraw=False, use it:
            self.sheet.set_sheet_data(visible_data, redraw=False)
            self.sheet.headers([self.df.columns[i] for i in visible_cols], redraw=False)
        except TypeError:
            self.sheet.set_sheet_data(visible_data)
            self.sheet.headers([self.df.columns[i] for i in visible_cols])

        # Restore scroll position
        try:
            MT.xview_moveto(x0)
            MT.yview_moveto(y0)
            self._sync_headers()
        except Exception:
            pass

        # Restore widths only if column count unchanged; otherwise let tksheet recalc
        try:
            if old_w is not None and old_count == len(visible_cols):
                self._set_col_widths(old_w)
        except Exception:
            pass

        self._force_redraw()

    def _apply_row_order(self, new_order):
        if not hasattr(self, "sheet"):
            return
        self._table_order = new_order
        # Only rebuild the data matrix; headers & column visibility are unchanged
        visible_cols = [i for i, col in enumerate(self.df.columns) if self.col_vars[col].get()]
        ordered_rows = [self._row_cache[i] for i in new_order]
        visible_data = [[row[j] for j in visible_cols] for row in ordered_rows]
        try:
            self.sheet.set_sheet_data(visible_data, redraw=False)
        except TypeError:
            self.sheet.set_sheet_data(visible_data)
        self._force_redraw()

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
                "columns": self.get_selected_table_columns(),
                "window_state": state,
            }

            if state == "normal":
                cfg["window_size"] = self.root.geometry()

            # Save collapsible states
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
