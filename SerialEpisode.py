# 
# SerialEpisode
# Class to represent a serial episode. 
# 

import copy
import functools

from Event import Event
from EpisodeSet import EpisodeSet

class SerialEpisode:
    
    ######################################################################
    # Construction
    ######################################################################
    def __init__(self, eventlst= []):
        """
        Constructs a new serial episode from the given list.
        """
        # Extracting the events
        self.events     = copy.deepcopy(eventlst)
        self.__len__    = len(eventlst)
        self.length     = self.__len__
        self.starttime  = 0 if self.length == 0 else eventlst[-1].time 
        self.endtime    = 0 if self.length == 0 else eventlst[0].time 
        # Creating the representation
        self.createRepr()
        
    def __show__(self): 
        # Show attributes
        print '####################################################'
        print '# episode attribute:'
        print '# events:',self.events     
        print '# start time:',self.starttime
        print '# end time:',self.endtime
        print '# length:',self.__len__
        print '# repr:',self.repr
        print '# prefix:',self.createPrefix().repr
        print '####################################################'
    
    
    ######################################################################
    # Manipulation (abandon)
    ######################################################################
    def add(self, event):
        """
        Adds an event to the episode. 
        """
        self.events.append(event)
        self.createRepr()
        
    
    def extend(self, events):
        """
        Adds the list of events to the episode. 
        """
        self.events.extend(events)
        self.createRepr()
    
    
    ######################################################################
    # Information Extraction
    ######################################################################
    def subeps(self):
        '''
        Generate all sub episodes of this episode.
		'''
        return self.extractSubEps(SerialEpisode([]))

    def extractSubEps(self,prefix):
        '''
        extract sub episodes of this episode recursively.
		'''
        sub_epsset = EpisodeSet([])  
        if prefix.length == 0:
            for i in range(self.length):
               pfx = [ self.events[i] ]
               pep = SerialEpisode(pfx) # prefix episode  
               #print pep.repr
               sub_epsset.add(pep) 
               #sub_eps[pep.repr] = sub_eps[pep.repr]+1 if sub_eps.has_key(pep.repr) else 1 
               sep = SerialEpisode(self.events[i+1:self.length]) # suffix episodes
               sub_epsset.merge(sep.extractSubEps(pep))
        else: 
            for i in range(self.length):
               pfx = copy.deepcopy(prefix.events)
               pfx.extend([ self.events[i] ])   
               pep = SerialEpisode(pfx) # prefix episode  
               #print pep.repr
               sub_epsset.add(pep) 
               #sub_eps[pep.repr] = sub_eps[pep.repr]+1 if sub_eps.has_key(pep.repr) else 1 
               sep = SerialEpisode(self.events[i+1:self.length]) # suffix episodes
               sub_epsset.merge(sep.extractSubEps(pep))
        return sub_epsset

    def getEvents(self):
        """
        Returns the list of all events in this serial episode. 
        """
        return self.events
    
    
    def empty(self):
        """
        Checks whether the serial episode is empty?
        """
        return self.length == 0
    
    
    def singleton(self):
        """
        Does this serial episode consist of a single event?
        """
        return self.length == 1
    
    
    def containsEvent(self, event):
        """
        Checks whether the given event is in this serial episode. 
        """
        return event in self.events
    
    
    def commonPrefix(self, other):
        """
        Checks whether this serail episode and the other episode have a common
        prefix of n-1. I.e. they differ only in the last event. 
        """
        if self.length == other.length:
            commonPrefix = True
            for i in range(0, self.length):
                if not self.events[i].__eq__(other.events[i]):
                    commonPrefix = False
                    break
            
            return commonPrefix and self.events[-1] != other.events[-1]
        else:
            return False
    
    def commonExceptOne(self, other):
        """
        Checks whether this episode and the other episode share n-1 events and 
        have a different nth event. 
        """
        return len([e for e in self.events if e in other.events]) \
                == len(self.events)-1
    
    
    ######################################################################
    # Object related
    ######################################################################
    
    def createPrefix(self):
        """
        Create prefix of this episode.
        """
        return SerialEpisode([]) if self.length == 0 else SerialEpisode(self.events[1:self.length]) 

    def createRepr(self):
        """
        Makes a simple representation of the serial episode . 
        """
        #self.repr = "<"
        self.repr = ""
        if self.length > 0:
            self.repr += (self.events[0]).type
            #self.repr += "%s,%s>"%((self.events[0]).type,self.events[0].time)
            for i in range(1,self.length):
                self.repr = (self.events[i]).type +"->" + self.repr
                #self.repr = "<%s,%s>->%s"%((self.events[i]).type,self.events[i].time,self.repr)
        
    
    def __repr__(self):
        """
        Returns the representation of this itemset
        """
        return self.repr
    
    
    def __eq__(self, other):
        """
        Are two episode equal?
        """
        if other != None and self.length == other.length:
            for i in range(0, self.length):
                if not self.events[i].__eq__(other.events[i]):
                   return False 
        else:
            return False
        return True

    def __sr__(self, other):
        """
        Are two episode have same reprentations ?
        """
        if other != None and self.length == other.length:
            return self.repr == other.repr
        else:
            return False
    
    def __hash__(self):
        """
        Hashes this episode 
        """
        return self.repr.__hash__()
    
if __name__ == '__main__':
    # [<A,5>, <D,4>, <A,2>, <B,1>] 
    #event1 = Event("A,1")
    #event2 = Event("B,2")
    #event3 = Event("C,3")
    #event4 = Event("D,4")
    #event5 = Event("E,8")
    #lst = []
    #lst.insert(0,event1)
    #lst.insert(0,event2)
    #lst.insert(0,event3)
    #lst.insert(0,event4)
    #lst.insert(0,event5)
    #eps = SerialEpisode(lst)
    #eps.__show__() 
    #eps.subeps()
    #lst[0] = Event("D,6") 
    #del lst[1] 
    #eps2 = SerialEpisode(lst)
    #eps.__show__() 
    #eps2.__show__() 
    #eps2.createPrefix().__show__() 
    #print eps.__eq__(eps2)
    #print eps.__sr__(eps2)
    #eps3 = SerialEpisode([])
    #eps3.__show__() 
    #eps.subeps()
    #eps2.subeps()
    e1 = Event("A,1")
    e2 = Event("B,2")
    e3 = Event("C,3")
    e4 = Event("D,4")
    e5 = Event("A,5")

    e6 = Event("A,2")
    e7 = Event("C,3")
    e8 = Event("B,4")
    e9 = Event("C,4")
    e10 = Event("D,4")
    
    el1 = [e1,e2,e3,e4]
    el2 = [e3,e4,e5]
    el3 = [e2,e6,e7]
    el4 = [e6,e7,e8,e8]

    print el1
    print el2
    print el3
    print el4

    eps1 = SerialEpisode(el1)
    eps2 = SerialEpisode(el2)
    eps3 = SerialEpisode(el3)
    eps4 = SerialEpisode(el4)

    epss1 = EpisodeSet([eps1,eps2]) 
    epss2 = EpisodeSet([eps1,eps4]) 
   
    epss1.__show__()
    epss2.__show__()
    
    print epss1.commonRatio(epss2) 
    
    
