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

        # build window
        self.top = Toplevel(app.root)
        self.top.title("User Search")
        self.top.transient(app.root)
        self.top.grab_set()
        self.top.protocol("WM_DELETE_WINDOW", self._on_close)

        # Center popup
        self.top.update_idletasks()
        parent_x = self.app.root.winfo_x()
        parent_y = self.app.root.winfo_y()
        parent_w = self.app.root.winfo_width()
        parent_h = self.app.root.winfo_height()
        w, h = 700, 450
        x = parent_x + (parent_w // 2 - w // 2)
        y = parent_y + (parent_h // 2 - h // 2)
        self.top.geometry(f"{w}x{h}+{x}+{y}")

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
        self.entry = ctk.CTkEntry(control_frame, placeholder_text="Type a name or ID…", width=250)
        self.entry.pack(side="left", padx=5)
        self.entry.bind("<Return>", lambda e: self.run_search())

        self.search_btn = ctk.CTkButton(control_frame, text="Search", command=self.run_search)
        self.search_btn.pack(side="left", padx=5)

        # ---- Bottom: two scrollable frames side by side ----
        lists_frame = ctk.CTkFrame(self.top, fg_color="transparent")
        lists_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Left list (unique names / user_id)
        self.user_listbox = ctk.CTkScrollableFrame(lists_frame, width=300, label_text="Users")
        self.user_listbox.pack(side="left", fill="both", expand=True, padx=5)

        # Right list (esp_ble_id for selected user)
        self.device_listbox = ctk.CTkScrollableFrame(lists_frame, width=300, label_text="esp_ble_id")
        self.device_listbox.pack(side="left", fill="both", expand=True, padx=5)

        # Store df from search
        self.search_results = pd.DataFrame()

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
        if field == "email":
            sql = text(f"""
                SELECT DISTINCT 
                    ucd.esp_ble_id, up.user_id, up.full_name
                FROM cpdevdb.user_cp_devices AS ucd
                JOIN cpdevdb.user_profile AS up 
                    ON ucd.user_id = up.user_id
                JOIN cpdevdb.user AS u
                    ON up.user_id = u.user_id
                WHERE u.email LIKE :pattern
                LIMIT {limit};

            """)
        else:
            sql = text(f"""
                SELECT DISTINCT up.user_id, up.full_name, ucd.esp_ble_id
                FROM cpdevdb.user_cp_devices AS ucd
                JOIN cpdevdb.user_profile AS up
                  ON ucd.user_id = up.user_id
                WHERE up.{field} LIKE :pattern
                LIMIT {limit};
            """)

        params = {"pattern": f"%{pattern}%"}  # substring search

        with self.query_manager.engine.connect() as conn:
            result = conn.execute(sql, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if df.empty:
            self.logger(f"No matches for {field}='{pattern}'")
        else:
            self.logger(f"Found {len(df)} results for {field}='{pattern}'")

        return df

    # -------------------------------
    # Worker + UI updates
    # -------------------------------
    def run_search(self):
        query_text = self.entry.get().strip()
        if not query_text:
            messagebox.showwarning("Empty search", "Please enter a value.")
            return
        field = self.search_mode.get()
        threading.Thread(target=self._search_worker, args=(field, query_text), daemon=True).start()

    def _search_worker(self, field: str, query_text: str):
        try:
            df = self._search_db(field, query_text, limit=200)

            # normalize NaN/null esp_ble_id
            if "esp_ble_id" in df.columns:
                df["esp_ble_id"] = df["esp_ble_id"].fillna("Device not connected")

            self.search_results = df
            self.top.after(0, lambda: self._update_user_list(df, query_text, field))
        except Exception as e:
            self.logger(f"[UserSearch] error: {e}")
            self.top.after(0, lambda err=e: messagebox.showerror("Error", str(err)))

    def _show_devices_for_user(self, user_id):
        # Clear device list
        for widget in self.device_listbox.winfo_children():
            widget.destroy()

        df = self.search_results
        subset = df[df["user_id"] == user_id]

        if subset.empty:
            ctk.CTkLabel(self.device_listbox, text="No devices for this user.").pack(anchor="w", padx=5, pady=5)
            return

        for _, row in subset.iterrows():
            esp = row["esp_ble_id"]

            if esp == "Device not connected":
                # Show a gray, non-clickable label
                ctk.CTkLabel(
                    self.device_listbox,
                    text="Device not connected",
                    anchor="w",
                    text_color="gray"
                ).pack(fill="x", padx=5, pady=2)
            else:
                # Show normal clickable button
                btn = ctk.CTkButton(
                    self.device_listbox,
                    text=esp,
                    anchor="w",
                    command=lambda esp_id=esp: self._select_esp_ble_id(esp_id)
                )
                btn.pack(fill="x", padx=5, pady=2)

    # -------------------------------
    # UI population
    # -------------------------------
    def _update_user_list(self, df, query_text, field):
        # Clear both frames
        for widget in self.user_listbox.winfo_children():
            widget.destroy()
        for widget in self.device_listbox.winfo_children():
            widget.destroy()

        if df.empty:
            ctk.CTkLabel(self.user_listbox, text=f"No results for {field} like '{query_text}'").pack(anchor="w", padx=5,
                                                                                                     pady=5)
            return

        # Ensure unique users
        unique_users = df.drop_duplicates(subset=["user_id", "full_name"]).copy()
        if not unique_users.empty:
            # Normalize capitalization
            unique_users["full_name"] = unique_users["full_name"].str.title()

            # Split into first/last for sorting
            unique_users[["first_name", "last_name"]] = unique_users["full_name"].str.split(" ", n=1, expand=True)
            # Sort case-insensitive
            unique_users = unique_users.sort_values(
                by=["first_name", "last_name"],
                key=lambda col: col.str.lower()
            )
            # Drop helpers
            unique_users = unique_users.drop(columns=["first_name", "last_name"])

        # Populate UI
        for _, row in unique_users.iterrows():
            uid = row["user_id"]
            name = row["full_name"]
            btn = ctk.CTkButton(
                self.user_listbox,
                text=f"{name} (ID={uid})",
                anchor="w",
                command=lambda u=uid: self._show_devices_for_user(u)
            )
            btn.pack(fill="x", padx=5, pady=2)

    def _show_devices_for_user(self, user_id):
        # Clear device list
        for widget in self.device_listbox.winfo_children():
            widget.destroy()

        df = self.search_results
        subset = df[df["user_id"] == user_id]

        if subset.empty:
            ctk.CTkLabel(self.device_listbox, text="No devices for this user.").pack(anchor="w", padx=5, pady=5)
            return

        for _, row in subset.iterrows():
            esp = row["esp_ble_id"]
            btn = ctk.CTkButton(
                self.device_listbox,
                text=esp,
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

