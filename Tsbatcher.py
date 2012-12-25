# 
# Tsbatcher
# Batch of events based on Time Span in a window. 
# 

import copy
import functools

from Event import Event
from SerialEpisode import SerialEpisode 
from EpisodeSet import EpisodeSet


class Tsbatcher(list):
    
   
    def __init__(self, interval, window):
        """
        Constructor
        """
        # Calling the super
        super(Tsbatcher, self).__init__()
        
        # Storing values
        self.length    = 0
        self.interval  = interval
        self.starttime = 0
        self.endtime   = self.starttime
        self.window    = window
        self.winIndex  = -1
        self.epsset    = EpisodeSet([]) 
       
	######################################################################
    # Generate Episodes
    ######################################################################
    def episodeStatistic(self):
        '''
        Generate episode set in current Window with this time span batch.
		'''
        while len(self.window) + self.winIndex >= 0:
            self.move()
            seq = [ e for e in self ]
            #print seq
            serialepisode = SerialEpisode(seq)
            self.epsset.merge(serialepisode.subeps())
        return self.epsset 
            
    def extractEpisodes(self,prefix,sequence):
        '''
        Generate episodes in current time span batch.
		'''
        if len(prefix) == 0:
            for i in range(len(sequence)):
               pfx = [ sequence[i] ]
               print pfx
               tsq = copy.deepcopy(sequence[i+1:len(sequence)])
               self.extractEpisodes(pfx,tsq)
        else: 
            for i in range(len(sequence)):
               pfx = copy.deepcopy(prefix)
               pfx.extend([ sequence[i] ])   
               print pfx
               tsq = copy.deepcopy(sequence[i+1:len(sequence)])
               self.extractEpisodes(pfx,tsq)

    ######################################################################
    # Management
    ######################################################################
    def move(self):
        """
        Move the batch along the window. 
        """
        out = None
        if len(self) > 0:
            out = self[-1]
            del self[-1]
            if len(self) == 0:
			    self.starttime += 1
            else:        	
                self.starttime = self[-1].time
    
        #event = Event("X,0")	
        while len(self.window) + self.winIndex >= 0:
            event = self.window[self.winIndex]
            if len(self) == 0:
                self.insert(0, event)
                self.starttime = event.time
                self.endtime = event.time
                self.winIndex -= 1
            else:
                if event.time - self.starttime <= self.interval:
    	            self.endtime = event.time
                    self.insert(0,event)
    	            self.winIndex -= 1	
                else:
                    break
        #print "batch:"+str(self)		
   

    def filled(self):
        """
        Returns whether the window is full (i.e. contains length 
        elements). 
        """
        return len(self) == self.length
    
    
    ######################################################################
    # Frequency
    ######################################################################
    def getCount(self, event):
        """
        Returns the count of the given event in this window. 
        """
        # Attempt 2: Still too slow
        count = 0
        
        for mEvent in self:
            if event.__st__(mEvent):
                count += 1
        
        return count
        
        # Attempt 1: Too slow
        #return reduce((lambda x, y: x+y),
        #            map((lambda i: itemset <= i), self))
    
    
    ######################################################################
    # Getters
    ######################################################################
    def getStreamEventTypes(self):
        return self.streamEventTypes
    
    
    
