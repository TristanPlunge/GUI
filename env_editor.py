import customtkinter as ctk
import keyring

SERVICE_NAME = "PlungeTubApp"


class EnvEditor(ctk.CTkToplevel):
    DEFAULTS = {
        "SSH_HOST": "54.204.114.213",
        "SSH_PORT": "22",
        "REMOTE_BIND_PORT": "3306",
        "MYSQL_DB": "cparchivedb"
    }

    def __init__(self, master, required_keys):
        super().__init__(master)
        self.title("Configure SSH / Database Connection")
        self.geometry("480x480+250+250")
        self.resizable(False, False)
        self.grab_set()  # make modal

        self.entries = {}
        self.saved = False

        ctk.CTkLabel(
            self,
            text="Please enter your SSH and Database connection details:",
            font=("Arial", 14)
        ).pack(pady=15)

        container = ctk.CTkFrame(self, corner_radius=12)
        container.pack(fill="both", expand=True, padx=20, pady=10)

        for key in required_keys:
            row = ctk.CTkFrame(container, fg_color="transparent")
            row.pack(fill="x", pady=5)

            ctk.CTkLabel(row, text=key + ":", width=140, anchor="w").pack(side="left", padx=5)
            entry = ctk.CTkEntry(
                row,
                width=280,
                show="*" if "PASS" in key else None,
                placeholder_text=f"Enter {key}"
            )

            # ✅ prefill order: keyring → defaults → empty
            saved_val = keyring.get_password(SERVICE_NAME, key)
            if saved_val:
                entry.insert(0, saved_val)
            elif key in self.DEFAULTS:
                entry.insert(0, self.DEFAULTS[key])

            entry.pack(side="left", padx=5)
            self.entries[key] = entry

        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(fill="x", pady=15)

        save_btn = ctk.CTkButton(btn_frame, text="Save & Continue", command=self.on_save)
        save_btn.pack(side="right", padx=15)

        cancel_btn = ctk.CTkButton(btn_frame, text="Cancel", fg_color="gray", command=self.on_cancel)
        cancel_btn.pack(side="right", padx=5)

    def on_save(self):
        for k, entry in self.entries.items():
            val = entry.get().strip()
            if val:
                keyring.set_password(SERVICE_NAME, k, val)
            else:
                keyring.delete_password(SERVICE_NAME, k)

        self.saved = True
        self.destroy()

    def on_cancel(self):
        """Just close without saving"""
        self.saved = False
        self.destroy()
