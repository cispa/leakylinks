import os
import subprocess
import time
import contextlib
from threading import Semaphore
import re
import logging

class DisplayManager:
    def __init__(self, base_display=10, num_displays=2, screen='1280x800x24', max_sessions_per_display=5):
        self.base_display = base_display
        self.num_displays = num_displays
        self.screen = screen
        self.max_sessions_per_display = max_sessions_per_display
        self.xvfb_procs = {}  # display_id -> proc
        
        # Add semaphore-based concurrency control
        self.semaphores = {}
        for i in range(num_displays):
            display_id = base_display + i
            self.semaphores[display_id] = Semaphore(max_sessions_per_display)
        
        # Pre-start all displays
        for i in range(num_displays):
            display_id = base_display + i
            self.start_xvfb(display_id)

    def start_xvfb(self, display_id):
        # Check if an Xvfb process for this display is already running
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            for line in result.stdout.splitlines():
                if f'Xvfb :{display_id}' in line:
                    logging.debug(f"Xvfb already running on :{display_id}, skipping start.")
                    return  # Already running externally
        except Exception as e:
            logging.debug(f"Failed to check for existing Xvfb on :{display_id}: {e}")

        xvfb_cmd = [
            "Xvfb",
            f":{display_id}",
            "-screen", "0", self.screen,
            "-nolisten", "tcp"
        ]
        try:
            proc = subprocess.Popen(xvfb_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(0.5)
            if proc.poll() is not None:
                return
            else:
                self.xvfb_procs[display_id] = proc
                logging.debug(f"Started Xvfb on :{display_id}")
        except Exception as e:
            # Suppress repeated noisy logs, log only once per display
            if not hasattr(self, '_xvfb_warned'):
                self._xvfb_warned = set()
            if display_id not in self._xvfb_warned:
                logging.debug(f"[DisplayManager] Xvfb may already be running on :{display_id} — skipping start. Error: {e}")
                self._xvfb_warned.add(display_id)

    def ensure_display(self, display_id):
        if display_id not in self.xvfb_procs or self.xvfb_procs[display_id].poll() is not None:
            self.start_xvfb(display_id)

    def get_display_id(self, session_index):
        return self.base_display + (session_index % self.num_displays)

    @contextlib.contextmanager
    def acquire_display(self, session_index):
        """Smart display acquisition with concurrency control"""
        display_id = self.get_display_id(session_index)
        semaphore = self.semaphores[display_id]
        
        if semaphore.acquire(blocking=False):
            try:
                self.ensure_display(display_id)
                yield display_id
            finally:
                semaphore.release()
        else:
            # Fallback: try any available display
            for disp_id, sem in self.semaphores.items():
                if sem.acquire(blocking=False):
                    try:
                        self.ensure_display(disp_id)
                        yield disp_id
                    finally:
                        sem.release()
                    return
            # If all displays are busy, wait for the original one
            semaphore.acquire(blocking=True)
            try:
                self.ensure_display(display_id)
                yield display_id
            finally:
                semaphore.release()

    def cleanup_xvfb_pool(self):
        base_display = self.base_display
        num_displays = self.num_displays
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            lines = result.stdout.splitlines()
            for line in lines:
                m = re.search(r'Xvfb\s+:(\d+)', line)
                if m:
                    display_num = int(m.group(1))
                    if base_display <= display_num < base_display + num_displays:
                        pid = int(line.split()[1])
                        logging.debug(f"Killing Xvfb on :{display_num} (PID {pid}) [managed pool]")
                        try:
                            subprocess.run(['kill', '-9', str(pid)])
                        except Exception as e:
                            logging.warning(f"Failed to kill Xvfb :{display_num} (PID {pid}): {e}")
        except Exception as e:
            logging.warning(f"Error during Xvfb pool cleanup: {e}")

    def cleanup(self):
        for display_id, proc in self.xvfb_procs.items():
            if proc.poll() is None:
                proc.terminate()
                logging.info(f"Stopped Xvfb on :{display_id}") 
        self.cleanup_xvfb_pool() 