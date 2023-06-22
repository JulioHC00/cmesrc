import tkinter as tk
from tkinter import ttk, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.animation import FuncAnimation
from matplotlib.figure import Figure
import matplotlib
import os
import numpy as np
from matplotlib.colors import Normalize
import zarr
import sqlite3

class AnimationReview:
    def __init__(self, root, db_filepath, zarr_path):
        self.root = root
        self.root.protocol("WM_DELETE_WINDOW", self.close_program)
        self.conn = sqlite3.connect(db_filepath)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS decisions (
            harpnum INTEGER PRIMARY KEY references harps(harpnum),
            decision TEXT
        )""")

        self.animation_dir = zarr_path
        self.animation_files = sorted(os.listdir(self.animation_dir))
        self.animation_index_dict = {f: i for i, f in enumerate(self.animation_files)}
        self.indices = np.arange(len(self.animation_files))
        self.processed_indices = set()
        self.current_index = None

        # Check which have already been processed

        self.cursor.execute("SELECT harpnum FROM decisions")

        for row in self.cursor.fetchall():
            self.processed_indices.add(self.animation_index_dict[f"{row[0]}"])

        self.fig = Figure()
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        self.menu = ttk.Combobox(self.root, values=self.animation_files)
        self.menu.pack(side=tk.TOP)

        self.progress_bar = ttk.Progressbar(self.root, length=200, mode='determinate', maximum=len(self.animation_files))
        self.progress_bar.pack(side=tk.TOP)


        self.counter = tk.Label(self.root, text=f"0/{len(self.animation_files)}")
        self.counter.pack(side=tk.TOP)

        # Update progress bar and counter
        self.progress_bar["value"] = len(self.processed_indices)
        self.counter["text"] = f"{len(self.processed_indices)}/{len(self.animation_files)}"

        self.frame_delay = [100]
        self.slider = tk.Scale(self.root, from_=10, to=600, orient=tk.HORIZONTAL, command=self.set_frame_delay)
        self.slider.pack(side=tk.TOP)
        self.slider.set(self.frame_delay[0])

        self.root.bind("<y>", self.yes)
        self.root.bind("<n>", self.no)
        self.root.bind("<<ComboboxSelected>>", self.select_animation)

        self.title_text = self.ax.text(0.5, 1.01, "", transform=self.ax.transAxes, ha="center", va="bottom")
        self.warning_text = self.ax.text(0.5, 1.11, "", fontsize=20, transform=self.ax.transAxes, ha="center", va="bottom", color="red")
        self.animation = None

        self.im = None
        self.arr = None
        self.quiv = None
        self.current_frame = 0

        self.after_id = None

        # Add reset button that clears processed indices and resets the progress bar
        self.reset_button = tk.Button(self.root, text="Reset", command=self.reset)

        # Add button that says start to go to first non processed index
        self.start_button = tk.Button(self.root, text="Start", command=self.start)

        # Put reset and start side to side
        self.reset_button.pack(side=tk.LEFT)
        self.start_button.pack(side=tk.LEFT)

        # Add counter of rejected
        self.rejected_counter = tk.Label(self.root, text=f"Rejected: 0")
        self.rejected_counter.pack(side=tk.RIGHT)
        self.update_rejected()

        # Add buttons to go back and forth
        self.back_button = tk.Button(self.root, text="Back", command=self.back)
        self.forward_button = tk.Button(self.root, text="Forward", command=self.forward)
        self.forward_button.pack(side=tk.RIGHT)
        self.back_button.pack(side=tk.RIGHT)


    def forward(self):
        if self.current_index is not None:
            self.current_index += 1
        else:
            self.current_index = 0

        self.execute_animation()

    def back(self):
        if self.current_index is not None:
            self.current_index -= 1
            self.current_index = max(self.current_index, 0)
        else:
            self.current_index = len(self.animation_files) - 1

        self.execute_animation()


    def start(self):
        self.next_animation()

    def reset(self):
        self.processed_indices = set()
        self.progress_bar["value"] = 0
        self.counter["text"] = f"0/{len(self.animation_files)}"


    def set_frame_delay(self, val):
        self.frame_delay[0] = int(val)

    def clear_animation(self):
        if self.animation is not None:
            self.animation.event_source.stop()
        self.ax.cla()
        self.animation = None
        self.title_text = self.ax.text(0.5, 1.01, "", transform=self.ax.transAxes, ha="center", va="bottom")  # Create a new text object
        self.warning_text = self.ax.text(0.5, 1.11, "", fontsize=20, transform=self.ax.transAxes, ha="center", va="bottom", color="red")

    def display_animation(self, im, arr, quiv):
        try:
            self.im = im
            self.arr = arr
            self.quiv = quiv
            self.current_frame = 0
            self.canvas.draw()
            self.update()
        except Exception as e:
            messagebox.showerror("Error", str(e))
#        try:
#            self.animation = FuncAnimation(self.fig, self.animate, fargs=(im, arr, quiv), frames=arr.shape[0], repeat=True, interval=4000/15)
#            self.animation.event_source.interval = self.frame_delay[0]
#            self.canvas.draw()
#        except Exception as e:
#            messagebox.showerror("Error", str(e))

    def update(self):
        self.animate(self.current_frame, self.im, self.arr, self.quiv)
        self.current_frame += 1

        if self.current_frame >= self.arr.shape[0]:
            self.current_frame = 0
        self.canvas.draw()
        self.after_id =  self.root.after(self.frame_delay[0], self.update)

    def animate(self, i, im, arr, quiv):
        # Removes quiver
        for coll in self.ax.collections:
            if isinstance(coll, matplotlib.quiver.Quiver):
                self.ax.collections.remove(coll)
                break

        img = arr[i, 2]

        attrs = arr.attrs["timestamps"]
        timestamp = attrs[i]
        im.set_data(img)

        # Update quiver plot

        vx = arr[i, 0, :, :]
        vy = arr[i, 1, :, :]

        dimx = vx.shape[1]
        dimy = vy.shape[0]

        X, Y = np.meshgrid(np.arange(dimx), np.arange(dimy))

        U = vx.flatten()
        V = vy.flatten()

        bz = arr[i,2, :, :].flatten()

        magnitudes = np.hypot(U, V)

        max_magnitude = magnitudes.max()

        thresh_magnitude = np.percentile(magnitudes, 90)

        colors = ['red' if b > 0 else 'blue' for b in bz]
        alphas = [1 if m > thresh_magnitude else 0 for m in magnitudes]

#        quiv = self.ax.quiver(X, Y, U, V, color=colors, pivot='tail', units='xy', scale=0.25 * max_magnitude, headwidth=4, headlength=6, headaxislength=5, alpha=alphas)

#        quiv.set_UVC(U, V)

        self.title_text.set_text(timestamp)  # set text for the text object here

    def yes(self, event):
        self.record_decision('yes')
        self.next_animation()

    def no(self, event):
        self.record_decision('no')
        self.update_rejected()
        self.next_animation()

    def get_rejected_count(self):
        sql_command = """SELECT COUNT(*) FROM decisions WHERE decision = 'no'"""
        self.cursor.execute(sql_command)
        return self.cursor.fetchone()[0]

    def update_rejected(self):
        self.rejected_counter['text'] = f"Rejected: {self.get_rejected_count()}"
        self.rejected_counter.update()

    def record_decision(self, decision):
        try:
            print(f"Chose {decision}")
            # Insert or modify if already exists
            sql_command = """INSERT OR REPLACE INTO decisions (harpnum, decision) VALUES (?, ?)"""
            self.cursor.execute(sql_command, (self.animation_files[self.current_index], decision))
            self.conn.commit()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def next_animation(self):
        self.processed_indices.add(self.current_index)
        not_processed_indices = list(set(self.indices) - self.processed_indices)

        self.progress_bar['value'] = len(self.processed_indices)
        self.counter['text'] = f"{len(self.processed_indices)}/{len(self.animation_files)}"

        if len(not_processed_indices) > 0:
            self.current_index = not_processed_indices[0]
            self.execute_animation()
        else:
            messagebox.showinfo("Done", "All animations reviewed.")

    def execute_animation(self):
        self.clear_animation()

        if self.after_id is not None:
            self.root.after_cancel(self.after_id)
            self.after_id = None

        current_animation_path = os.path.join(self.animation_dir, self.animation_files[self.current_index])
        store = zarr.DirectoryStore(current_animation_path)
        arr = zarr.open(store, mode="r")

        global_min = np.percentile(arr[:, 2, :, :], 0.5)
        global_max = np.percentile(arr[:, 2, :, :], 99.5)



        norm = Normalize(vmin=global_min, vmax=global_max)
        tot = arr[0, 1, :, :]
        im = self.ax.imshow(arr[0, 1, :, :], norm=norm, cmap='gray', origin="lower")

        # Check if any dimension over 224
        if arr.shape[2] > 224 or arr.shape[3] > 224:
            # If so, print in red that this image is too big
            self.warning_text.set_text("Image too big")

        vx = arr[0, 0, :, :]
        vy = arr[0, 1, :, :]

        dimx = vx.shape[1]
        dimy = vy.shape[0]

        X, Y = np.meshgrid(np.arange(dimx), np.arange(dimy))

        U = vx.flatten()
        V = vy.flatten()

        magnitudes = np.hypot(U, V)

        max_magnitude = magnitudes.max()

        quiv = self.ax.quiver(X, Y, U, V, color='red', pivot='mid', units='xy', scale=0.5 * max_magnitude, headwidth=6, headlength=6, headaxislength=5, alpha=0)

        self.fig.suptitle(self.animation_files[self.current_index])

        self.display_animation(im, arr, quiv)

    def select_animation(self, event):
        self.current_index = self.animation_files.index(self.menu.get())
        self.execute_animation()

    def close_program(self):
        self.conn.close()
        self.root.destroy()

DB_FILEPATH = "/home/julio/cmesrc/data/processed/cmesrc.db"
ZARR_PATH = "/home/julio/cmesrc/data/processed/cutouts/cutouts/"

root = tk.Tk()
app = AnimationReview(root, DB_FILEPATH, ZARR_PATH)
root.mainloop()
