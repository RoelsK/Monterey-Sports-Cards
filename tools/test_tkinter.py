import tkinter as tk

def test_tkinter():
    window = tk.Tk()
    window.title("Test Tkinter")
    label = tk.Label(window, text="If you see this, Tkinter is working!")
    label.pack()
    window.mainloop()

test_tkinter()
