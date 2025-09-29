import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# === CONFIG ===
COMPRESSOR_THRESHOLD = 1.0   # amps (used to detect ON/OFF state)
MAX_EXPECTED_DURATION = 30   # minutes
MIN_EXPECTED_DURATION = 5    # minutes
MIN_COOLING_RATE = 0.2       # °F per minute

# Compressor current expectations (raw dataset values)
MIN_COMPRESSOR_AMP = 2.3     # amps (too low if below this)
MAX_COMPRESSOR_AMP = 4.0     # amps (too high if above this)

COLUMNS = [
    "updated_at",
    "water_temp_f",
    "target_temp_f",
    "pump_current_amp",
    "compressor_current_amp"
]

class CycleAnalyzerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Plunge Tub Cycle Analyzer")

        # Menu
        menubar = tk.Menu(root)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Open CSV", command=self.open_csv)
        menubar.add_cascade(label="File", menu=file_menu)
        root.config(menu=menubar)

        # --- Date range selectors ---
        date_frame = tk.Frame(root)
        date_frame.pack(fill="x", pady=5)

        tk.Label(date_frame, text="Start Date (YYYY-MM-DD):").pack(side="left", padx=5)
        self.start_entry = tk.Entry(date_frame, width=12)
        self.start_entry.pack(side="left", padx=5)

        tk.Label(date_frame, text="End Date (YYYY-MM-DD):").pack(side="left", padx=5)
        self.end_entry = tk.Entry(date_frame, width=12)
        self.end_entry.pack(side="left", padx=5)

        # Button to run analyzer
        self.analyze_btn = tk.Button(date_frame, text="Analyze", command=self.run_with_range, state="disabled")
        self.analyze_btn.pack(side="left", padx=10)

        # Text box for logs
        self.text = tk.Text(root, wrap="word", height=10)
        self.text.pack(fill="x", expand=False)

        # Placeholder for matplotlib canvas
        self.canvas_frame = tk.Frame(root)
        self.canvas_frame.pack(fill="both", expand=True)
        self.canvas = None

        self.df = None  # stores loaded data

    def open_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not path:
            return
        try:
            df = pd.read_csv(path, parse_dates=["updated_at"])
            missing = [c for c in COLUMNS if c not in df.columns]
            if missing:
                messagebox.showerror("Error", f"Missing required columns: {missing}")
                return
            self.df = df.sort_values("updated_at").reset_index(drop=True)
            self.text.insert("end", f"✅ Loaded {len(df)} rows\n")
            # Enable the analyze button
            self.analyze_btn.configure(state="normal")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {e}")

    def run_with_range(self):
        if self.df is None:
            messagebox.showerror("Error", "Please load a CSV first.")
            return

        start_str = self.start_entry.get().strip()
        end_str = self.end_entry.get().strip()

        try:
            if start_str:
                start_dt = pd.to_datetime(start_str)
                df = self.df[self.df["updated_at"] >= start_dt]
            else:
                df = self.df.copy()

            if end_str:
                end_dt = pd.to_datetime(end_str)
                df = df[df["updated_at"] <= end_dt]

            if df.empty:
                self.text.insert("end", "⚠️ No data in selected range.\n")
                return

            self.analyze_cycles(df)

        except Exception as e:
            messagebox.showerror("Error", f"Invalid date input: {e}")

    def analyze_cycles(self, df):
        self.text.insert("end", f"\nAnalyzing {len(df)} rows...\n")

        cycles = []
        in_cycle = False
        start_idx = None

        for i, row in df.iterrows():
            compressor_on = row["compressor_current_amp"] > COMPRESSOR_THRESHOLD
            if compressor_on and not in_cycle:
                in_cycle = True
                start_idx = i
            elif not compressor_on and in_cycle:
                in_cycle = False
                end_idx = i
                cycles.append((start_idx, end_idx))

        if not cycles:
            self.text.insert("end", "No cooling cycles detected.\n")
            return

        self.text.insert("end", f"Detected {len(cycles)} cooling cycles\n")

        # Create matplotlib figure
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(df["updated_at"], df["water_temp_f"], label="Water Temp (°F)")
        ax.plot(df["updated_at"], df["target_temp_f"], label="Target Temp (°F)", linestyle="--")
        ax.plot(df["updated_at"], df["compressor_current_amp"], label="Compressor Current", alpha=0.6)
        ax.plot(df["updated_at"], df["pump_current_amp"], label="Pump Current", alpha=0.6)

        for (s, e) in cycles:
            start_time = df.loc[s, "updated_at"]
            end_time = df.loc[e, "updated_at"]
            duration_min = (end_time - start_time).total_seconds() / 60
            start_temp = df.loc[s, "water_temp_f"]
            end_temp = df.loc[e, "water_temp_f"]
            cooling_rate = (start_temp - end_temp) / max(duration_min, 1e-3)
            target_temp = df.loc[s:e, "target_temp_f"].min()

            anomaly_reasons = []
            if duration_min > MAX_EXPECTED_DURATION:
                anomaly_reasons.append("too long")
            if duration_min < MIN_EXPECTED_DURATION:
                anomaly_reasons.append("too short")
            if cooling_rate < MIN_COOLING_RATE:
                anomaly_reasons.append("cooling too slow")
            if end_temp > target_temp + 1:
                anomaly_reasons.append("target not reached")

            # New: Check compressor amps
            avg_current = df.loc[s:e, "compressor_current_amp"].mean()
            if avg_current < MIN_COMPRESSOR_AMP:
                anomaly_reasons.append("compressor current too low")
            elif avg_current > MAX_COMPRESSOR_AMP:
                anomaly_reasons.append("compressor current too high")

            color = "green" if not anomaly_reasons else "red"
            ax.axvspan(start_time, end_time, color=color, alpha=0.2)

            summary = (
                f"Cycle {start_time:%m-%d %H:%M} → {end_time:%m-%d %H:%M} | "
                f"{duration_min:.1f} min, rate={cooling_rate:.2f} °F/min, "
                f"avg_current={avg_current:.2f} A"
            )
            if anomaly_reasons:
                summary += " ⚠️ " + ", ".join(anomaly_reasons)
            else:
                summary += " ✅ normal"
            self.text.insert("end", summary + "\n")

        ax.set_xlabel("Time")
        ax.set_ylabel("Values")
        ax.legend()
        fig.tight_layout()

        # Clear old canvas
        for widget in self.canvas_frame.winfo_children():
            widget.destroy()

        # Embed matplotlib into Tkinter
        self.canvas = FigureCanvasTkAgg(fig, master=self.canvas_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill="both", expand=True)


if __name__ == "__main__":
    root = tk.Tk()
    app = CycleAnalyzerGUI(root)
    root.mainloop()
