import sys
import logging

class StatusBar:
  """reports the progress as you update it"""
  def __init__(self, tasks, delim="-"):
    self.delim = delim
    self.length = 20
    self.cursor = 0
    self.tasks = tasks
    self.progress = 0
    self.percent = 0.0
  def __enter__(self):
    """sets up the status bar"""
    sys.stdout.write("[{}]\r".format(" " * self.length))
    sys.stdout.flush()
    return self
  def updateone(self, msg =""):
    """adds a tick to the status bar with optional message"""
    self.progress += 1
    self.percent = self.progress/float(self.tasks)
    if self.percent > 0.05 * self.cursor:
      self.cursor += 1
    sys.stdout.write("[{}] {:.0f}% {}\r".format(self.delim * self.cursor + " " * (self.length - self.cursor), self.percent * 100, msg + " "))
    sys.stdout.flush()
  def updatemsg(self, msg):
    sys.stdout.write("[{}] {:.0f}% {}\r".format(self.delim * self.cursor + " " * (self.length - self.cursor), self.percent * 100, msg + " "))
    sys.stdout.flush()
  def __exit__(self, exc_type, exc_value, traceback):
    sys.stdout.write("[{}] {:.0f}% {}\n".format(self.delim * self.cursor+ " " * (self.length - self.cursor), 100 , "DONE" + " "*100))
    sys.stdout.flush()

if __name__ == "__main__":
  import time
  limit = 250
  sbar = StatusBar(limit)
  with StatusBar(limit) as sbar:
    for i in range(limit):
      sbar.updateone()
      time.sleep(0.1)
      sbar.updatemsg("hello")
    
