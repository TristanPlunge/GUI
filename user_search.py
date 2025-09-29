# user_search.py
import customtkinter as ctk
from tkinter import Toplevel, messagebox
import threading
import pandas as pd
import config_manager
from sqlalchemy import text


class UserSearchWindow:
    def __init__(self, app, query_manager, logger=print):
        self.app = app                # MetricsApp instance
        self.query_manager = query_manager
        self.logger = logger
        self.selected_user_btn = None   # track currently selected user button
        # build window
        self.top = Toplevel(app.root)
        self.top.title("User Search")
        self.top.transient(app.root)
        self.top.grab_set()
        self.top.protocol("WM_DELETE_WINDOW", self._on_close)
        self.search_running = False
        # Center popup
        self.top.update_idletasks()
        parent_x = self.app.root.winfo_x()
        parent_y = self.app.root.winfo_y()
        parent_w = self.app.root.winfo_width()
        parent_h = self.app.root.winfo_height()
        w, h = 800, 450
        x = parent_x + (parent_w // 2 - w // 2)
        y = parent_y + (parent_h // 2 - h // 2)
        self.top.geometry(f"{w}x{h}+{x}+{y}")
        self.top.focus_force()
        # Close window when pressing Escape
        self.top.bind("<Escape>", lambda e: self._on_close())

        # ---- Top: search controls ----
        # ---- Top: search controls ----
        control_frame = ctk.CTkFrame(self.top, fg_color="transparent")
        control_frame.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(control_frame, text="Search by:", text_color="black").pack(side="left", padx=5)

        saved_mode = self.app.config.get("search_mode", "full_name")
        self.search_mode = ctk.StringVar(value=saved_mode)

        self.mode_menu = ctk.CTkComboBox(
            control_frame,
            variable=self.search_mode,
            values=["full_name", "user_id", "email"],
            width=150
        )
        self.mode_menu.pack(side="left", padx=5)

        self.search_mode.trace_add("write", self._on_mode_change)

        self.entry = ctk.CTkEntry(control_frame, placeholder_text="Enter values", width=200)
        self.entry.pack(side="left", padx=5)
        self.entry.bind("<Return>", lambda e: self.run_search())
        self.entry.focus_set()
        # ✅ keep button in same frame
        self.search_btn = ctk.CTkButton(control_frame, text="Search", command=self.run_search)
        self.search_btn.pack(side="left", padx=5)

        # ✅ status label aligned too
        self.status_label = ctk.CTkLabel(control_frame, text="", text_color="gray")
        self.status_label.pack(side="left", padx=10)

        # ---- Bottom: two scrollable frames side by side ----
        lists_frame = ctk.CTkFrame(self.top, fg_color="transparent")
        lists_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Left list (unique names / user_id)
        self.user_listbox = ctk.CTkScrollableFrame(lists_frame, width=300, label_text="Users")
        self.user_listbox.pack(side="left", fill="both", expand=True, padx=5)

        # Right list (esp_ble_id for selected user)
        self.device_listbox = ctk.CTkScrollableFrame(lists_frame, width=450, label_text="esp_ble_id")
        self.device_listbox.pack(side="left", fill="both", expand=True, padx=5)

        # Store df from search
        self.search_results = pd.DataFrame()

    def run_search(self):
        if self.search_running:
            messagebox.showinfo("Search Running", "A search is already in progress. Please wait.")
            return

        query_text = self.entry.get().strip()
        if not query_text:
            messagebox.showwarning("Empty search", "Please enter a value.")
            return

        field = self.search_mode.get()
        self.search_running = True
        self.status_label.configure(text="Searching...", text_color="black")
        self.search_btn.configure(state="disabled")  # disable button while running

        threading.Thread(target=self._search_worker, args=(field, query_text), daemon=True).start()

    def _on_close(self):
        try:
            # update parent config with latest dropdown selection
            self.app.config["search_mode"] = self.search_mode.get()
            config_manager.save_config(self.app.config)
        except Exception as e:
            self.logger(f"[UserSearch] Failed to save search_mode: {e}")
        finally:
            self.top.destroy()

    def _on_mode_change(self, *args):
        try:
            self.app.config["search_mode"] = self.search_mode.get()
            # If you want to save right away:
            if hasattr(self.app, "_save_config_now"):
                self.app._save_config_now()
            self.logger(f"[UserSearch] Updated search_mode={self.search_mode.get()}")
        except Exception as e:
            self.logger(f"[UserSearch] Failed to update search_mode: {e}")

    # -------------------------------
    # Query logic
    # -------------------------------
    def _search_db(self, field: str, pattern: str, limit: int = 100) -> pd.DataFrame:
        if not self.query_manager.connect():
            return pd.DataFrame()

        if field == "user_id":
            sql = text(f"""
                SELECT DISTINCT 
                    up.user_id, 
                    up.full_name, 
                    u.email,
                    ucd.esp_ble_id, 
                    ud.device_nickname
                FROM cpdevdb.user_profile AS up
                JOIN cpdevdb.user AS u
                    ON up.user_id = u.user_id
                LEFT JOIN cpdevdb.user_cp_devices AS ucd
                    ON ucd.user_id = up.user_id
                LEFT JOIN cpdevdb.user_device AS ud
                    ON ucd.user_id = ud.user_id
                WHERE up.user_id = :exact
                   OR up.user_id LIKE :pattern
                LIMIT {limit};
            """)
            params = {
                "exact": pattern.strip(),
                "pattern": f"%{pattern.strip()}%"
            }

        elif field == "email":
            sql = text(f"""
                SELECT DISTINCT 
                    up.user_id, 
                    up.full_name, 
                    u.email,
                    ucd.esp_ble_id, 
                    ud.device_nickname
                FROM cpdevdb.user_profile AS up
                JOIN cpdevdb.user AS u
                    ON up.user_id = u.user_id
                LEFT JOIN cpdevdb.user_cp_devices AS ucd
                    ON ucd.user_id = up.user_id
                LEFT JOIN cpdevdb.user_device AS ud
                    ON ucd.user_id = ud.user_id
                WHERE u.email LIKE :pattern
                LIMIT {limit};
            """)
            params = {"pattern": f"%{pattern.strip()}%"}

        else:  # full_name or other fields
            tokens = pattern.strip().split()
            like_clauses = " AND ".join(
                [f"up.{field} LIKE :token{i}" for i in range(len(tokens))]
            )
            sql = text(f"""
                SELECT DISTINCT 
                    up.user_id, 
                    up.full_name, 
                    u.email,
                    ucd.esp_ble_id, 
                    ud.device_nickname
                FROM cpdevdb.user_profile AS up
                JOIN cpdevdb.user AS u
                    ON up.user_id = u.user_id
                LEFT JOIN cpdevdb.user_cp_devices AS ucd
                    ON ucd.user_id = up.user_id
                LEFT JOIN cpdevdb.user_device AS ud
                    ON ucd.user_id = ud.user_id
                WHERE {like_clauses}
                LIMIT {limit};
            """)
            params = {f"token{i}": f"%{t}%" for i, t in enumerate(tokens)}

        with self.query_manager.engine.connect() as conn:
            result = conn.execute(sql, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    # -------------------------------
    # Worker + UI updates
    # -------------------------------

    def _search_worker(self, field: str, query_text: str):
        try:
            df = self._search_db(field, query_text, limit=200)

            if "esp_ble_id" in df.columns:
                df["esp_ble_id"] = df["esp_ble_id"].fillna("Device not connected")

            self.search_results = df
            self.top.after(0, lambda: self._update_user_list(df, query_text, field))
        except Exception as e:
            self.logger(f"[UserSearch] error: {e}")
            self.top.after(0, lambda err=e: messagebox.showerror("Error", str(err)))
        finally:
            def finish():
                self.search_running = False
                self.status_label.configure(text="Done", text_color="green")
                self.search_btn.configure(state="normal")

            self.top.after(0, finish)

    # -------------------------------
    # UI population
    # -------------------------------
    def _update_user_list(self, df, query_text, field):
        # Clear frames
        for widget in self.user_listbox.winfo_children():
            widget.destroy()
        for widget in self.device_listbox.winfo_children():
            widget.destroy()

        if df.empty:
            ctk.CTkLabel(
                self.user_listbox,
                text=f"No results for {field} like '{query_text}'"
            ).pack(anchor="w", padx=5, pady=5)
            return

        users = df.drop_duplicates(subset=["user_id", "full_name"]).copy()
        if not users.empty:
            users["full_name"] = users["full_name"].str.title()

            if field == "user_id":
                # --- User ID search: exact match to top ---
                users["priority"] = users["user_id"].astype(str).apply(
                    lambda uid: 0 if uid == query_text.strip() else 1
                )
                users = users.sort_values(by=["priority", "user_id"])
            else:
                # --- Full name / email search ---
                suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "dr", "dr."}

                def split_name(name):
                    parts = name.split()
                    if not parts:
                        return "", ""
                    first = parts[0]
                    # strip suffixes
                    while parts and parts[-1].lower().strip(".") in suffixes:
                        parts = parts[:-1]
                    last = parts[-1] if len(parts) > 1 else ""
                    return first, last

                users[["first_name", "last_name"]] = users["full_name"].apply(
                    lambda n: pd.Series(split_name(n))
                )

                # Normalize query tokens
                query_tokens = query_text.strip().title().split()

                def priority_fn(row):
                    first, last = row["first_name"], row["last_name"]

                    # Exact match on first only (e.g., "Guy")
                    if len(query_tokens) == 1 and query_tokens[0] == first:
                        return 0
                    # Exact match on first+last (e.g., "Guy B" matches "Guy Buf")
                    if len(query_tokens) == 2 and query_tokens[0] == first and last.startswith(query_tokens[1]):
                        return 0
                    # Fallback: contains query anywhere
                    if any(q in row["full_name"] for q in query_tokens):
                        return 1
                    return 2

                users["priority"] = users.apply(priority_fn, axis=1)

                # Sort by priority, then first/last
                users = users.sort_values(
                    by=["priority", "first_name", "last_name"],
                    key=lambda col: col.str.lower() if col.dtype == "object" else col
                )

        for _, row in users.iterrows():
            uid = row["user_id"]
            name = row["full_name"]
            email = row.get("email", "N/A")

            container = ctk.CTkFrame(self.user_listbox, fg_color="transparent")
            container.pack(fill="x", padx=5, pady=2)

            # Name + ID (clickable button)
            btn = ctk.CTkButton(
                container,
                text=f"{name} (ID={uid})",
                anchor="w",
                font=ctk.CTkFont(size=14, weight="bold")
            )
            btn.configure(command=lambda u=uid, b=btn: self._on_user_click(u, b))
            btn.pack(fill="x")

            # Email as subtitle
            email_label = ctk.CTkLabel(
                container,
                text=email,
                anchor="w",
                text_color="white",
                font=ctk.CTkFont(size=12)
            )
            email_label.pack(fill="x", padx=10)  # indent for better alignment

    def _on_user_click(self, user_id, btn):
        if self.selected_user_btn is not None and self.selected_user_btn.winfo_exists():
            self.selected_user_btn.configure(text_color="white")

        btn.configure(text_color="yellow")  # highlight
        self.selected_user_btn = btn
        self._show_devices_for_user(user_id)

    def _show_devices_for_user(self, user_id):
        # Clear device list
        for widget in self.device_listbox.winfo_children():
            widget.destroy()

        df = self.search_results
        subset = df[df["user_id"] == user_id]

        if subset.empty:
            ctk.CTkLabel(
                self.device_listbox,
                text="No devices for this user."
            ).pack(anchor="w", padx=5, pady=5)
            return

        user_name = subset["full_name"].iloc[0] if "full_name" in subset.columns else "Unknown"
        user_email = subset["email"].iloc[0] if "email" in subset.columns else "No e-mail"

        header_label = ctk.CTkLabel(
            self.device_listbox,
            text=f"{user_name} (ID={user_id})\n{user_email}",
            font=ctk.CTkFont(size=16, weight="bold"),
            text_color=("black", "white"),
            fg_color=("#E0E0E0", "#333333"),
            corner_radius=6,
            anchor="center",
            justify="center",
            width=280,
            height=50
        )
        header_label.pack(pady=(0, 10), padx=5)

        # Populate device buttons
        for _, row in subset.iterrows():
            esp = row["esp_ble_id"]
            nickname = row.get("device_nickname")

            if esp == "Device not connected":
                btn = ctk.CTkButton(
                    self.device_listbox,
                    text="Device not connected",
                    anchor="w",
                    state="disabled",
                    text_color="gray"
                )
                btn.pack(fill="x", padx=5, pady=2)
            else:
                label_text = esp
                if nickname and pd.notna(nickname):
                    label_text = f"{esp} ({nickname})"
                else:
                    label_text = f"{esp} (No name)"

                btn = ctk.CTkButton(
                    self.device_listbox,
                    text=label_text,
                    anchor="w",
                    command=lambda esp_id=esp: self._select_esp_ble_id(esp_id)
                )
                btn.pack(fill="x", padx=5, pady=2)

    def _select_esp_ble_id(self, esp_id):
        if esp_id in ("No device", "Device not connected"):
            # do nothing if it's a placeholder
            return
        """When an esp_ble_id is clicked, push it into the parent filter and close search."""
        try:
            # Update MetricsApp filters
            self.app.filter_type.set("esp_ble_id")
            self.app.filter_value.delete(0, "end")
            self.app.filter_value.insert(0, esp_id)

            self.logger(f"[UserSearch] Selected esp_ble_id={esp_id}")
        except Exception as e:
            self.logger(f"[UserSearch] Failed to set esp_ble_id: {e}")
        finally:
            self.top.destroy()

