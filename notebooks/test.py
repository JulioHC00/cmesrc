import tkinter as tk
from tkinter import messagebox
import zarr
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.animation as animation
from matplotlib.colors import Normalize


ani = None
canvas = None

# Function to handle plotting and animation
def animate(i, im, ax, arr):
    img = arr[i, 2]

    # Read attrs
    attrs = arr.attrs["timestamps"]
    timestamp = attrs[i]
    im.set_data(img)
    ax.set_title(timestamp)

# Function to handle ID submission
def submit():
    id = id_entry.get()
    try:
        path = f"/home/julio/cmesrc/data/processed/cutouts/cutouts/{id}"
        store = zarr.DirectoryStore(path)
        arr = zarr.open(store, mode='r')

        global_min = np.percentile(arr[:, 2, :, :], 0.5)
        global_max = np.percentile(arr[:, 2, :, :], 99.5)

        norm = Normalize(vmin=global_min, vmax=global_max)

        fig, ax = plt.subplots()
        im = ax.imshow(arr[0, 2, :, :], norm=norm, cmap='gray')
        ax.set_title(arr.attrs["timestamps"][0])

        global canvas
        if canvas:
            canvas.get_tk_widget().pack_forget()
        canvas = FigureCanvasTkAgg(fig, master=root)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        global ani

        FPS = 12
        ani = animation.FuncAnimation(fig, animate, fargs=(im, ax, arr), frames=arr.shape[0], repeat=True, interval=1000/FPS)
    except Exception as e:
        messagebox.showerror("Error", str(e))
        id_entry.delete(0, 'end')

# Creating the GUI
root = tk.Tk()
root.title("Zarr Store Animator")

# Creating the entry field for ID input
id_label = tk.Label(root, text="Enter the ID:")
id_label.pack()
id_entry = tk.Entry(root)
id_entry.pack()

# Creating the submit button
submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.pack()

root.mainloop()
