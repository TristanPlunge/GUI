import pandas as pd
import numpy as np
import customtkinter as ctk
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.widgets import RectangleSelector
from datetime import timedelta, datetime
import json, os



class PlotManager:
    def __init__(self, output_frame, on_select=None, on_key=None, max_points=5000):
        self.output_frame = output_frame
        self.fig = None
        self.ax = None
        self.canvas = None
        self.selector = None
        self.tooltip_enabled = True  # default ON

        # Data store
        self.current_df = pd.DataFrame()
        self.current_columns = []
        self.line_colors = {}
        self.lines = {}  # col -> Line2D

        # Crosshair + tooltip
        self.vline = None
        self._tooltip = None

        # X time caches (for quick search)
        self._x_pd = None                    # pandas datetime Series (tz-naive)
        self._x_np = None                    # numpy datetime64[ns] array (sorted)                 # matplotlib float date numbers (optional)
        self._ds_idx = None                  # downsample indices for plotting

        # Store "home view"
        self.home_xlim = None

        # Hooks
        self.on_select_hook = on_select
        self.on_key_hook = on_key

        # Cache files
        self.cache_file = "plot_cache.parquet"
        self.meta_file = "plot_cache_meta.json"

        # Perf knobs
        self.max_points = int(max_points)    # hard cap on points per line

    def show_message(self, msg: str, color="red"):
        """Display a centered message on the plot instead of data."""
        if self.fig is None or self.ax is None or self.canvas is None:
            self.init_plot()
        self.ax.clear()
        self.ax.text(
            0.5, 0.5, msg,
            ha="center", va="center", transform=self.ax.transAxes,
            fontsize=14, color=color, wrap=True
        )
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.canvas.draw_idle()

    # -------------------------------
    # Init plot
    # -------------------------------
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

    def init_plot(self):
        # Container frame for plot + toolbar
        self.plot_container = ctk.CTkFrame(self.output_frame)
        self.plot_container.pack(fill="both", expand=True, padx=10, pady=10)

        # Create figure + canvas
        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_container)

        # Toolbar (must be created AFTER canvas, and explicitly packed)
        self.toolbar = NavigationToolbar2Tk(self.canvas, self.plot_container)
        self.toolbar.update()
        self.toolbar.pack(side="bottom", fill="x")  # explicit pack, don't rely on default

        # Canvas widget
        self.canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        # Rectangle selector
        self.selector = RectangleSelector(
            self.ax, self._on_select,
            useblit=True, interactive=False,
            drag_from_anywhere=True, button=[1]
        )

        # Events
        self.fig.canvas.mpl_connect("motion_notify_event", self._on_mouse_motion)
        self.fig.canvas.mpl_connect("key_press_event", self._on_key)
        self.fig.canvas.mpl_connect("scroll_event", self._on_scroll)
        self.fig.canvas.mpl_connect("button_press_event", self._on_mouse_press)
        self.fig.canvas.mpl_connect("button_release_event", self._on_mouse_release)
        self.fig.canvas.mpl_connect("axes_enter_event", self._on_axes_enter)
        self.fig.canvas.mpl_connect("figure_leave_event", self._on_figure_leave)

        # Labels & formatting
        self.ax.set_title("Device Metrics")
        self.ax.set_xlabel("Updated at (Los Angeles)")
        self.ax.set_ylabel("Values")
        self.ax.margins(x=0)

        locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
        formatter = mdates.DateFormatter("%m/%d")
        self.ax.xaxis.set_major_locator(locator)
        self.ax.xaxis.set_major_formatter(formatter)
        self.fig.autofmt_xdate(rotation=0, ha="center")
    def _on_axes_enter(self, event):
        # restore normal tooltip mode
        self._tooltip_mode = "cursor"

    # -------------------------------
    # Plot data (fast)
    # -------------------------------
    def plot_data(self, df, selected_columns=None, fresh=False, color_map=None, col_states=None):
        # Always trust col_states if provided
        if col_states is not None:
            selected_columns = [col for col, state in col_states.items() if state]
        self.current_columns = list(selected_columns or [])
        # This is the *only* source of truth
        selected = set(selected_columns or [])
        self.current_columns = list(selected)

        if df is None:
            return

        # Break time gaps
        df = self._break_time_gaps(df, threshold="1D")

        # Ensure plot exists
        if self.fig is None or self.ax is None or self.canvas is None:
            self.init_plot()

        if fresh:
            self.lines.clear()
            self.vline = None
            self._tooltip = None
            self.ax.clear()
            self.current_df = pd.DataFrame()

            # Handle empty selection immediately
            if not selected:
                self._draw_fixed_legend()
                self.canvas.draw_idle()
                return

            # Normal datetime prep
            if "updated_at" not in df.columns:
                self.ax.text(0.5, 0.5, "No results",
                             ha="center", va="center", transform=self.ax.transAxes,
                             fontsize=14, color="red")
                self.canvas.draw_idle()
                return

            df = df.copy()
            if not pd.api.types.is_datetime64_any_dtype(df["updated_at"]):
                df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

            df = df.dropna(subset=["updated_at"]).sort_values("updated_at")
            self.current_df = df
            self._x_pd = df["updated_at"]
            self._x_np = self._x_pd.values.astype("datetime64[ns]")

        # 🔥 Rebuild exactly what’s selected
        # Remove all old lines first
        for line in list(self.lines.values()):
            line.remove()
        self.lines.clear()

        if not selected:
            self._draw_fixed_legend()
            self.canvas.draw_idle()
            return

        # Add back only checked columns
        for col in selected:
            if col in self.current_df.columns and pd.api.types.is_numeric_dtype(self.current_df[col]):
                series = pd.to_numeric(self.current_df[col], errors="coerce")
                sub = pd.DataFrame({"updated_at": self._x_pd, col: series})
                if self._ds_idx is not None:
                    sub = sub.iloc[self._ds_idx]
                line, = self.ax.plot(
                    sub["updated_at"], sub[col],
                    label=col,
                    color=(color_map.get(col) if color_map and col in color_map else None),
                )
                self.lines[col] = line
                self.line_colors[col] = line.get_color()

        # Update legend
        self._draw_fixed_legend()
        self.canvas.draw_idle()

    def _save_cache(self, df, col_states=None):
        try:
            cols = list(df.columns)
            if "updated_at" not in cols:
                cols.append("updated_at")

            if "updated_at" in df.columns and not df.empty:
                earliest = pd.to_datetime(df["updated_at"]).min()
                latest = pd.to_datetime(df["updated_at"]).max()
                earliest_iso = earliest.isoformat()
                latest_iso = latest.isoformat()
            else:
                earliest_iso = latest_iso = None

            # Always write parquet (or fallback to CSV)
            try:
                df[cols].to_parquet(self.cache_file, index=False)
                fmt = "parquet"
            except (ImportError, ValueError):
                alt_file = os.path.splitext(self.cache_file)[0] + ".csv"
                df[cols].to_csv(alt_file, index=False)
                self.cache_file = alt_file
                fmt = "csv"

            # Save plot + column state into config.json
            plot_state = {
                "col_states": col_states or {},
                "xlim": list(self.ax.get_xlim()) if self.ax else None,
                "ylim": list(self.ax.get_ylim()) if self.ax else None,
                "time_range": {"earliest": earliest_iso, "latest": latest_iso},
                "format": fmt,
            }

            import config_manager
            cfg = config_manager.load_config()
            cfg["plot_state"] = plot_state
            config_manager.save_config(cfg)

            return True
        except Exception as e:
            print(f"[Cache] Failed to save: {e}")
            return False

    def load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                df = pd.read_parquet(self.cache_file)
                return df
            except Exception as e:
                print(f"[Cache] Failed to load: {e}")
        return None

    # -------------------------------
    # Tooltip + Crosshair (fast nearest)
    # -------------------------------
    def _on_mouse_motion(self, event):
        if getattr(self, "_is_panning", False):
            self._on_mouse_drag(event)
        else:
            self._on_mouse_move(event)

    def _on_mouse_move(self, event):
        if not self.tooltip_enabled:
            return
        if getattr(self, "_tooltip_mode", "cursor") == "legend":
            return  # don’t draw cursor tooltips if docked
        if event.inaxes != self.ax or event.xdata is None or self.current_df.empty:
            return

        # Fast nearest: binary search in sorted _x_np
        try:
            if len(self._x_np) == 0:
                return

            mouse_dt_py = mdates.num2date(event.xdata).replace(tzinfo=None)
            mouse_ns = np.datetime64(mouse_dt_py, "ns")

            if mouse_ns < self._x_np[0] or mouse_ns > self._x_np[-1]:
                # Outside data range → fabricate a "zero row"
                row = {col: 0 for col in self.current_columns}
                row["updated_at"] = mouse_dt_py
            else:
                # Normal nearest neighbor logic
                idx = int(np.searchsorted(self._x_np, mouse_ns))
                if idx <= 0:
                    nearest = 0
                elif idx >= len(self._x_np):
                    nearest = len(self._x_np) - 1
                else:
                    prev_diff = abs(self._x_np[idx - 1] - mouse_ns)
                    next_diff = abs(self._x_np[idx] - mouse_ns)
                    nearest = idx - 1 if prev_diff <= next_diff else idx
                row = self.current_df.iloc[nearest]
        except Exception:
            return

        # Clean up old tooltip annotations
        if hasattr(self, "_tooltip_items"):
            for item in self._tooltip_items:
                try:
                    item.remove()
                except Exception:
                    pass
        self._tooltip_items = []

        # First line = timestamp (white)
        lines = [row["updated_at"].strftime("%Y-%m-%d %H:%M:%S")]
        colors = ["white"]

        if "device_name" in row and pd.notna(row["device_name"]):
            lines.append(f"Device: {row['device_name']}")
            colors.append("white")  # or "white" if you want consistent

        # Add metric values with their line colors
        for col in self.current_columns:
            if col in row and pd.notna(row[col]):
                try:
                    val_str = f"{float(row[col]):.2f} : {col}"
                except Exception:
                    val_str = f"{row[col]} : {col}"
                lines.append(val_str)
                colors.append(self.line_colors.get(col, "white"))

        if not lines:
            return

        # Background box (no visible text, just black rounded pad)
        bg = self.ax.annotate(
            "\n".join(lines),
            xy=(event.xdata, event.ydata),
            xytext=(30, 10),
            textcoords="offset points",
            fontsize=9,
            color="none",
            va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.8", fc="black", alpha=0.85),
            zorder=200,
        )
        self._tooltip_items.append(bg)

        # Overlay each line with its color
        line_height = 12
        y_offset_start = 12
        for i, (text, c) in enumerate(zip(lines, colors)):
            t = self.ax.annotate(
                text,
                xy=(event.xdata, event.ydata),
                xytext=(34, y_offset_start - i * line_height),
                textcoords="offset points",
                fontsize=9,
                color=c,
                va="top", ha="left",
                zorder=201,
            )
            self._tooltip_items.append(t)

        # Vertical crosshair
        if self.vline is None:
            self.vline = self.ax.axvline(event.xdata, color="gray", linestyle="--")
        else:
            self.vline.set_xdata([event.xdata, event.xdata])

        self.canvas.draw_idle()

    # -------------------------------
    # Legend (visible-only)
    # -------------------------------
    def _draw_fixed_legend(self):
        vis_lines = [ln for ln in self.ax.get_lines() if ln.get_visible()]
        if vis_lines:
            labels = [ln.get_label() for ln in vis_lines]
            self.ax.legend(
                vis_lines, labels,
                loc="center left", bbox_to_anchor=(1.01, 0.5),
                frameon=True, fontsize=9
            )
            self.fig.subplots_adjust(right=0.8)
        else:
            leg = self.ax.get_legend()
            if leg:
                leg.remove()
            self.fig.subplots_adjust(right=0.95)

    # -------------------------------
    # Panning
    # -------------------------------
    def _on_mouse_press(self, event):
        if event.button == 3 and event.inaxes == self.ax:
            self._is_panning = True
            self._pan_start_px = (event.x, event.y)
            self._orig_xlim = self.ax.get_xlim()
            self._orig_ylim = self.ax.get_ylim()

    def _on_mouse_release(self, event):
        if event.button == 3:
            self._is_panning = False

    def _on_figure_leave(self, event):
        # Clear tooltip items
        if hasattr(self, "_tooltip_items"):
            for item in self._tooltip_items:
                try:
                    item.remove()
                except Exception:
                    pass
            self._tooltip_items = []

        # Remove crosshair if present
        if self.vline is not None:
            try:
                self.vline.remove()
            except Exception:
                pass
            self.vline = None

        if self.canvas:
            self.canvas.draw_idle()

    def _on_mouse_drag(self, event):
        if not getattr(self, "_is_panning", False) or event.inaxes != self.ax:
            return
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
    # Zoom & Keys
    # -------------------------------
    def _on_select(self, eclick, erelease):
        if eclick.xdata is None or erelease.xdata is None:
            return

        x0, x1 = eclick.xdata, erelease.xdata
        y0, y1 = eclick.ydata, erelease.ydata

        # Only zoom if there’s actually a nonzero span
        if x0 != x1:
            self.ax.set_xlim(min(x0, x1), max(x0, x1))
        if y0 != y1:
            self.ax.set_ylim(min(y0, y1), max(y0, y1))

        self.canvas.draw_idle()
        if self.on_select_hook:
            self.on_select_hook(eclick, erelease)

    def _on_scroll(self, event):
        if event.inaxes != self.ax:
            return
        xlim, ylim = self.ax.get_xlim(), self.ax.get_ylim()
        xdata, ydata = event.xdata, event.ydata
        scale = 1.2 if event.button == "up" else 1 / 1.2
        new_xlim = [xdata - (xdata - xlim[0]) * scale, xdata + (xlim[1] - xdata) * scale]
        new_ylim = [ydata - (ydata - ylim[0]) * scale, ydata + (ylim[1] - ydata) * scale]
        self.ax.set_xlim(new_xlim)
        self.ax.set_ylim(new_ylim)
        self.canvas.draw_idle()

    def _on_key(self, event):
        key = event.key.lower() if event.key else ""

        # Tooltip toggle
        if key == "t":
            self.tooltip_enabled = not self.tooltip_enabled
            print(f"[PlotManager] Tooltip {'enabled' if self.tooltip_enabled else 'disabled'}")

            if not self.tooltip_enabled:
                if hasattr(self, "_tooltip_items"):
                    for item in self._tooltip_items:
                        try:
                            item.remove()
                        except Exception:
                            pass
                    self._tooltip_items = []
                if self.vline is not None:
                    self.vline.remove()
                    self.vline = None
                self.canvas.draw_idle()
            return

        # Reset view
        if key in ["escape", "r"]:
            self.reset_view()
            return

        # Arrow keys navigation
        xlim = self.ax.get_xlim()
        x0, x1 = mdates.num2date(xlim[0]), mdates.num2date(xlim[1])
        delta = timedelta(minutes=1)
        if key.startswith("shift"):
            delta = timedelta(hours=1)
        if key.startswith("ctrl"):
            delta = timedelta(days=1)

        if key.endswith("left"):
            self.ax.set_xlim(x0 - delta, x1 - delta)
        elif key.endswith("right"):
            self.ax.set_xlim(x0 + delta, x1 + delta)

        self.canvas.draw_idle()

        if self.on_key_hook:
            self.on_key_hook(event)

    # -------------------------------
    # Reset view
    # -------------------------------
    def reset_view(self):
        """Reset plot to show the full extent of the currently loaded dataset."""
        if self.current_df is None or self.current_df.empty:
            print("[PlotManager] No data loaded, cannot reset view.")
            return

        try:
            start = self.current_df["updated_at"].min()
            end = self.current_df["updated_at"].max()
            if start is None or end is None:
                print("[PlotManager] No valid updated_at values.")
                return

            # Reset x-limits
            self.ax.set_xlim(start, end)
            self.home_xlim = self.ax.get_xlim()

            # Rescale y based on visible data
            self.ax.relim(visible_only=True)
            self.ax.autoscale(axis="y", tight=False)

            self.canvas.draw_idle()
            print(f"[PlotManager] 🔄 Reset view to full range: {start} → {end}")
        except Exception as e:
            print(f"[PlotManager] ❌ Reset view failed: {e}")

    def set_time_window(self, start_la: datetime, end_la: datetime):
        """Force the x-axis to the exact LA-naive datetime window (matches UI inputs)."""
        if self.ax is None or start_la is None or end_la is None:
            return
        if end_la <= start_la:
            return
        self.ax.set_xlim(start_la, end_la)
        # make this the "home" view for Esc / 'r'
        self.home_xlim = self.ax.get_xlim()
        if self.canvas:
            self.canvas.draw_idle()

    # -------------------------------
    # Cache
    # -------------------------------
    # in PlotManager._save_cache
    def _break_time_gaps(self, df, threshold="1D"):
        """
        Insert NaN rows wherever time gaps exceed the threshold.
        Ensures matplotlib does not connect lines across missing data.
        """
        if df.empty or "updated_at" not in df.columns:
            return df

        df = df.sort_values("updated_at").copy()
        gap_threshold = pd.Timedelta(threshold)

        # Find indices where the gap is larger than threshold
        gaps = df["updated_at"].diff() > gap_threshold
        rows_to_insert = []

        for i in df[gaps].index:
            ts = df.loc[i, "updated_at"]

            # Build a dict of correct dtypes → NaN or NaT
            nan_row = {}
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    nan_row[col] = pd.NaT
                elif pd.api.types.is_numeric_dtype(df[col]):
                    nan_row[col] = np.nan
                else:
                    nan_row[col] = None
            nan_row["updated_at"] = ts - pd.Timedelta(seconds=1)

            rows_to_insert.append(nan_row)

        if rows_to_insert:
            filler = pd.DataFrame(rows_to_insert).astype(df.dtypes.to_dict(), errors="ignore")
            df = pd.concat([df, filler], ignore_index=True)
            df = df.sort_values("updated_at")

        return df


    def load_col_states(self):
        """Return only the cached checkbox states from meta file."""
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, "r") as f:
                    meta = json.load(f)
                return meta.get("col_states", {})
            except Exception as e:
                print(f"[Cache] Failed to load col_states: {e}")
        return {}
