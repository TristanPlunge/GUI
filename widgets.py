import customtkinter as ctk
import json
import os

CONFIG_FILE = "user_config.json"


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


def save_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


import customtkinter as ctk

class CollapsibleSection(ctk.CTkFrame):
    def __init__(self, master, title="Section", *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.is_expanded = True
        self.configure(fg_color="#1f6aa5")  # header background

        # Full-width toggle button
        self.header_btn = ctk.CTkButton(
            self, text=title, fg_color="transparent", anchor="w",
            command=self.toggle
        )
        self.header_btn.pack(fill="x", padx=2, pady=0, ipady=0, ipadx=0)

        # Content frame
        self.content = ctk.CTkFrame(self, corner_radius=10, fg_color="#144870")
        self.content.pack(fill="both", expand=True, padx=5, pady=5)

    def toggle(self):
        if self.is_expanded:
            self.content.forget()
        else:
            self.content.pack(fill="both", expand=True, padx=5, pady=5)
        self.is_expanded = not self.is_expanded

    def get_state(self):
        return "expanded" if self.is_expanded else "collapsed"

    def set_state(self, state: str):
        if state == "collapsed" and self.is_expanded:
            self.toggle()
        elif state == "expanded" and not self.is_expanded:
            self.toggle()




if __name__ == "__main__":
    app = ctk.CTk()
    app.geometry("400x300")

    section1 = CollapsibleSection(app, title="Section 1", section_id="sec1", collapsed=False)
    section1.pack(fill="x", pady=10)

    section2 = CollapsibleSection(app, title="Section 2", section_id="sec2", collapsed=True)
    section2.pack(fill="x", pady=10)

    app.mainloop()
