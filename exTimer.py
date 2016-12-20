# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 04:43:58 2016

@name  : ex_timer.py
@author: jihwan
"""

import time
import threading


class exTimer(threading.Thread):
 
    def __init__(self): 
        threading.Thread.__init__(self) 
        # default delay set.. 
        self.delay = 1
        self.state = True
        self.handler = None
 
    def setDelay(self, delay): 
        self.delay = delay
 
    def run(self): 
        while self.state: 
                time.sleep( self.delay ) 
                if self.handler != None: 
                        self.handler()
 
    def end(self): 
        self.state = False                
 
    def setHandler(self, handler): 
        self.handler = handler
