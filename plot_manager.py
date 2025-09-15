import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.widgets import RectangleSelector
from datetime import timedelta
import json, os


class PlotManager:
    def __init__(self, output_frame, on_select=None, on_key=None):
        self.output_frame = output_frame
        self.fig = None
        self.ax = None
        self.canvas = None
        self.selector = None

        # Data storage
        self.current_df = pd.DataFrame()
        self.current_columns = []
        self.line_colors = {}  # consistent colors for tooltips & legend

        # Crosshair + tooltip
        self.vline = None
        self.tooltip_items = []

        # Store "home view" (initial X range)
        self.home_xlim = None

        # Optional hooks
        self.on_select_hook = on_select
        self.on_key_hook = on_key

        # Cache file paths
        self.cache_file = "plot_cache.parquet"
        self.meta_file = "plot_cache_meta.json"

    # -------------------------------
    # Init plot
    # -------------------------------
    def init_plot(self):
        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.output_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        # Rectangle selector for zoom
        self.selector = RectangleSelector(
            self.ax,
            self._on_select,
            useblit=True,  # fast drawing
            interactive=False,  # shows handles, smoother resize
            drag_from_anywhere=True,  # optional convenience
            button=[1]
        )

        # Connect events
        self.fig.canvas.mpl_connect("key_press_event", self._on_key)
        self.fig.canvas.mpl_connect("scroll_event", self._on_scroll)
        self.fig.canvas.mpl_connect("motion_notify_event", self._on_mouse_move)

        self.fig.canvas.mpl_connect("button_press_event", self._on_mouse_press)
        self.fig.canvas.mpl_connect("button_release_event", self._on_mouse_release)
        self.fig.canvas.mpl_connect("motion_notify_event", self._on_mouse_drag)

    # -------------------------------
    # Plot data
    # -------------------------------
    def plot_data(self, df, selected_columns, fresh=False, color_map=None):
        if df is None:
            return

        if self.fig is None or self.ax is None:
            self.init_plot()

        df = df.copy()

        # Ensure datetime & drop bad rows
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df = df.dropna(subset=["updated_at"])

        # Coerce numeric columns
        for col in selected_columns:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Save for snapping
        self.current_df = df
        self.current_columns = selected_columns[:]

        if fresh:
            for widget in self.output_frame.winfo_children():
                widget.destroy()
            self.init_plot()
            preserve_limits = False
            vline_x = None  # nothing to restore yet
        else:
            xlim = self.ax.get_xlim()
            vline_x = None
            if self.vline is not None:
                vline_x = self.vline.get_xdata()[0]

            self.ax.clear()
            preserve_limits = True
            self.vline = None

        if df.empty:
            self.ax.text(
                0.5, 0.5, "No results",
                ha="center", va="center", transform=self.ax.transAxes,
                fontsize=14, color="red"
            )
            self.canvas.draw_idle()
            return

        # Store line colors
        self.line_colors = {}

        # Plot each selected column
        for col in selected_columns:
            if col in df and pd.api.types.is_numeric_dtype(df[col]):
                line, = self.ax.plot(
                    df["updated_at"],  # already LA naive datetime
                    df[col],
                    label=col,
                    color=color_map.get(col, None) if color_map else None
                )
                self.line_colors[col] = line.get_color()

        # Titles and labels
        self.ax.set_title("Device Metrics")
        self.ax.set_xlabel("Updated at (Los Angeles)")
        self.ax.set_ylabel("Values")

        # Force 2-line labels: YYYY-MM-DD on top, HH:MM:SS below
        locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
        formatter = mdates.DateFormatter("%Y-%m-%d\n%H:%M:%S")

        self.ax.xaxis.set_major_locator(locator)
        self.ax.xaxis.set_major_formatter(formatter)
        self.fig.autofmt_xdate(rotation=0, ha="center")  # keep them vertical aligned

        # Handle limits
        if preserve_limits:
            self.ax.set_xlim(xlim)
            self.ax.relim()
            self.ax.autoscale(axis="y")
        else:
            min_date = df["updated_at"].min()
            max_date = df["updated_at"].max()
            self.ax.set_xlim(min_date, max_date)
            self.home_xlim = self.ax.get_xlim()
            self._save_cache(df, selected_columns)

        # Re-add vline if it existed
        if vline_x is not None:
            self.vline = self.ax.axvline(vline_x, color="gray", linestyle="--")

        self._draw_fixed_legend()
        self.canvas.draw_idle()

    # -------------------------------
    # Mouse move handler (tooltips)
    # -------------------------------
    def _on_mouse_move(self, event):
        if event.inaxes != self.ax or event.xdata is None or event.ydata is None:
            return

        tooltip_lines, colors = [], []

        if not self.current_df.empty:
            # Convert mouse xdata → tz-naive datetime
            mouse_dt = pd.to_datetime(mdates.num2date(event.xdata))
            if mouse_dt.tzinfo is not None:
                mouse_dt = mouse_dt.tz_localize(None)

            idx_values = self.current_df["updated_at"]
            if pd.api.types.is_datetime64_any_dtype(idx_values):
                if getattr(idx_values.dt, "tz", None) is not None:
                    idx_values = idx_values.dt.tz_localize(None)

            # Nearest time index across DF
            distances = (idx_values - mouse_dt).abs()
            nearest_idx = distances.idxmin()
            nearest_time = self.current_df.loc[nearest_idx, "updated_at"]

            # Timestamp line
            tooltip_lines.append(nearest_time.strftime("%Y-%m-%d %H:%M:%S"))
            colors.append("white")

            # Values per column
            for col in self.current_columns:
                if col in self.current_df.columns:
                    val = self.current_df.loc[nearest_idx, col]
                    if pd.notna(val):
                        tooltip_lines.append(f"{val:.2f} : {col}")
                        colors.append(self.line_colors.get(col, "white"))
        else:
            tooltip_lines = [
                f"{mdates.num2date(event.xdata).strftime('%Y-%m-%d %H:%M:%S')}",
                f"{event.ydata:.2f} : Value"
            ]
            colors = ["white", "white"]

        # Remove old tooltips
        for item in getattr(self, "tooltip_items", []):
            try:
                item.remove()
            except Exception:
                item.set_visible(False)
        self.tooltip_items = []

        if not tooltip_lines:
            return

        # Background box with added padding for bottom line
        bg = self.ax.annotate(
            "\n".join(tooltip_lines),
            xy=(event.xdata, event.ydata),
            xytext=(10, 10),
            textcoords="offset points",
            fontsize=9,
            color="none",
            va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.8", fc="black", alpha=0.85),  # more pad
            zorder=200,
        )
        self.tooltip_items.append(bg)

        # Overlay colored lines
        line_height = 12  # pixel spacing
        y_offset_start = 12  # small bump to lift lines into bbox
        for i, (line, c) in enumerate(zip(tooltip_lines, colors)):
            t = self.ax.annotate(
                line,
                xy=(event.xdata, event.ydata),
                xytext=(14, y_offset_start - i * line_height),
                textcoords="offset points",
                fontsize=9,
                color=c,
                va="top", ha="left",
                zorder=201,
            )
            self.tooltip_items.append(t)

        # Vertical crosshair
        if self.vline is None:
            self.vline = self.ax.axvline(event.xdata, color="gray", linestyle="--")
        else:
            self.vline.set_xdata([event.xdata, event.xdata])

        self.canvas.draw_idle()

    # -------------------------------
    # Legend handling
    # -------------------------------
    def _draw_fixed_legend(self):
        if len(self.fig.axes) > 1:
            for extra_ax in self.fig.axes[1:]:
                extra_ax.remove()

        self.fig.subplots_adjust(left=0.1, right=0.78, bottom=0.2, top=0.9)
        legend_ax = self.fig.add_axes([0.79, 0.1, 0.2, 0.8])
        legend_ax.axis("off")

        handles, labels = self.ax.get_legend_handles_labels()
        legend_ax.legend(
            handles, labels,
            loc="center left",
            frameon=True,
            fontsize=9
        )
        self.fig.subplots_adjust(bottom=0.25)

    # -------------------------------
    # Panning
    # -------------------------------
    def _on_mouse_press(self, event):
        if event.button == 3 and event.inaxes == self.ax:  # right mouse
            self._is_panning = True
            self._pan_start_px = (event.x, event.y)
            self._orig_xlim = self.ax.get_xlim()
            self._orig_ylim = self.ax.get_ylim()

    def _on_mouse_release(self, event):
        if event.button == 3:
            self._is_panning = False

    def _on_mouse_drag(self, event):
        if getattr(self, "_is_panning", False) and event.inaxes == self.ax:
            dx_px = event.x - self._pan_start_px[0]
            dy_px = event.y - self._pan_start_px[1]

            inv = self.ax.transData.inverted()
            x0, y0 = inv.transform((0, 0))
            x1, y1 = inv.transform((dx_px, dy_px))
            dx_data, dy_data = x1 - x0, y1 - y0

            self.ax.set_xlim(self._orig_xlim[0] - dx_data, self._orig_xlim[1] - dx_data)
            self.ax.set_ylim(self._orig_ylim[0] - dy_data, self._orig_ylim[1] - dy_data)

            self.canvas.draw_idle()

    # -------------------------------
    # Other event handlers
    # -------------------------------
    def _on_select(self, eclick, erelease):
        if eclick.xdata is None or erelease.xdata is None:
            return
        self.ax.set_xlim(min(eclick.xdata, erelease.xdata), max(eclick.xdata, erelease.xdata))
        self.ax.set_ylim(min(eclick.ydata, erelease.ydata), max(eclick.ydata, erelease.ydata))
        self.canvas.draw_idle()
        if self.on_select_hook:
            self.on_select_hook(eclick, erelease)

    def _on_scroll(self, event):
        if event.inaxes != self.ax:
            return
        xlim = self.ax.get_xlim()
        ylim = self.ax.get_ylim()
        xdata, ydata = event.xdata, event.ydata
        scale_factor = 1.2 if event.button == "up" else 1 / 1.2
        new_xlim = [
            xdata - (xdata - xlim[0]) * scale_factor,
            xdata + (xlim[1] - xdata) * scale_factor,
        ]
        new_ylim = [
            ydata - (ydata - ylim[0]) * scale_factor,
            ydata + (ylim[1] - ydata) * scale_factor,
        ]
        self.ax.set_xlim(new_xlim)
        self.ax.set_ylim(new_ylim)
        self.canvas.draw_idle()

    def _on_key(self, event):
        if event.key in ["escape", "r"]:
            self.reset_view()
            return

        xlim = self.ax.get_xlim()
        x0, x1 = mdates.num2date(xlim[0]), mdates.num2date(xlim[1])
        delta = timedelta(minutes=1)
        if "shift" in event.key and "ctrl" not in event.key:
            delta = timedelta(hours=1)
        if "ctrl" in event.key:
            delta = timedelta(days=1)

        if "left" in event.key:
            x0, x1 = x0 - delta, x1 - delta
            self.ax.set_xlim(x0, x1)
            self.canvas.draw_idle()
        elif "right" in event.key:
            x0, x1 = x0 + delta, x1 + delta
            self.ax.set_xlim(x0, x1)
            self.canvas.draw_idle()

        if self.on_key_hook:
            self.on_key_hook(event)

    # -------------------------------
    # Reset view
    # -------------------------------
    def reset_view(self):
        if self.home_xlim:
            self.ax.set_xlim(self.home_xlim)
            self.ax.relim()
            self.ax.autoscale(axis="y")
            self.canvas.draw_idle()

    # -------------------------------
    # Cache
    # -------------------------------
    def _save_cache(self, df, selected_columns):
        try:
            df.to_parquet(self.cache_file)
            meta = {
                "columns": selected_columns,
                "xlim": list(self.ax.get_xlim()),
                "ylim": list(self.ax.get_ylim())
            }
            with open(self.meta_file, "w") as f:
                json.dump(meta, f)
        except Exception as e:
            print(f"[Cache] Failed to save: {e}")

    def load_cache(self):
        if os.path.exists(self.cache_file) and os.path.exists(self.meta_file):
            try:
                df = pd.read_parquet(self.cache_file)
                with open(self.meta_file, "r") as f:
                    meta = json.load(f)
                columns = meta.get("columns", [])
                self.plot_data(df, columns, fresh=True)
                if "xlim" in meta:
                    self.ax.set_xlim(meta["xlim"])
                if "ylim" in meta:
                    self.ax.set_ylim(meta["ylim"])
                self.canvas.draw_idle()
                return df, columns
            except Exception as e:
                print(f"[Cache] Failed to load: {e}")
        return None, None
