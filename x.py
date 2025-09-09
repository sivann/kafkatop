import time
import readchar
from threading import Thread
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

# A global flag to signal the threads to stop
stop_program = False

def check_for_quit():
    """
    Runs in a separate thread, waiting for 'q' to be pressed.
    """
    global stop_program
    # This will block until a key is pressed
    key = readchar.readkey()
    if key == 'q':
        stop_program = True

# Start the background thread to listen for the 'q' key
key_thread = Thread(target=check_for_quit, daemon=True)
key_thread.start()

content = Text("Running... Press 'q' to quit.", justify="center")
panel = Panel(content, title="Live Display", border_style="green")

# Use rich.live to continuously update the display
with Live(panel, refresh_per_second=4, screen=True) as live:
    counter = 0
    while not stop_program:
        time.sleep(0.25)
        counter += 1
        # Update the content of the panel
        new_text = Text(f"Running for {counter} steps...\nPress 'q' to quit.", justify="center")
        live.update(Panel(new_text, title="Live Display", border_style="green"))

print("Program finished.")
