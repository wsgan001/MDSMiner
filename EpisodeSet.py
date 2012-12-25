# 
# EpisodeSet
# Class to represent an episode set. Contains episodes and their representation, count 
# 

import copy
import functools
import math
from Event import Event
#from SerialEpisode import SerialEpisode

class EpisodeSet:
    
    ######################################################################
    # Construction
    ######################################################################
    def __init__(self, episodelst = []):
        """
        Constructs a new episode set from the given list.
        """
        # Extracting the episode
        self.episodes   = copy.deepcopy(episodelst)
        self.epscnt     = {}
        self.length     = 0
        # Creating the episode representation and its count
        self.createEpsCnt()
        
    def __show__(self): 
        # Show attributes
        print '####################################################'
        print '# episode set attribute:'
        print '# episodes:',self.episodes
        print '# episode and count:',self.epscnt
        print '# length:',self.length
        print '# maximum episode count:',self.maxEpsCnt()
        print '# minimumn episode count:',self.minEpsCnt()
        print '# mean episode count:',self.meanEpsCnt()
        print '# total episode count:',self.totalEpsCnt()
        print '####################################################'
    
    
    ######################################################################
    # Manipulation
    ######################################################################
    def add(self, episode):
        """
        Adds a serial episode to the episode. 
        """
        #for eps in self.episodes:
        #    if eps.__eq__(episode):
        #        return
        self.episodes.append(episode)
        self.createEpsCnt()
        
    
    #def extend(self, events):
    #    """
    #    Adds the list of events to the episode. 
    #    """
    #    self.events.extend(events)
    #    self.createRepr()
    
    def merge(self,other):
        """
        Merge self's {episode:count} dictionary with other's.
        """
        if other == None:
            return 
        for eps in other.episodes:
            self.add(eps)
        for key in other.epscnt.keys():
            self.epscnt[key] = self.epscnt[key] + other.epscnt[key] if self.epscnt.has_key(key) else other.epscnt[key]
        self.createEpsCnt()
        #self.__show__()

    def commonRatio(self,other):
        """
        Count the ratio of common episodes of the two episodes sets.
        """
        if other == None:
            return 0 
        n = 0   # number of common episodes 
        m = 0   # the smaller length of the two episodes set
        s = {}  # the {episode:count} with shorter length
        l = {}  # the {episode:count} with longer length

        if self.length < other.length:
            m = self.length
            s = self.epscnt
            l = other.epscnt
        else:
            m = other.length
            s = other.epscnt
            l = self.epscnt

        for key in s.keys():
            if key in l.keys():
                n += 1

        return 1.0*n/m        

    ######################################################################
    # Information Extraction
    ######################################################################
    def maxEpsCnt(self):
        """
        Returns the maximum episode count 
        and the episode of all episodes in this episode set. 
        """
        maxkey = '' 
        maxcnt = {1:0} 
        for key in self.epscnt.keys():
            val = self.epscnt[key]
            if val >= maxcnt[1]: 
              maxkey = key
              maxcnt[1] = val
        return {maxkey:maxcnt[1]} 

    def minEpsCnt(self):
        """
        Returns the minimum episode count 
        and the episode of all episodes in this episode set. 
        """
        minkey = self.epscnt.keys()[0] 
        mincnt = {1:self.epscnt.values()[0]} 
        for key in self.epscnt.keys():
            val = self.epscnt[key]
            if val < mincnt[1]: 
              minkey = key
              mincnt[1] = val
        return {minkey:mincnt[1]} 
   
    def totalEpsCnt(self):
        """
        Returns the total episode count 
        of all episodes in this episode set. 
        """
        tcnt = 0 
        for val in self.epscnt.values():
            tcnt += val 
  
        return tcnt
  
    def meanEpsCnt(self):
        """
        Returns the mean episode count 
        and the episode of all episodes in this episode set. 
        """
        return 1.0*self.totalEpsCnt()/self.length

    def computeSupport(self):
        """
        Compute supports of episodes in this episode set. 
        """
        epssupp = {} 
        tcnt = self.totalEpsCnt() 
        for key in self.epscnt.keys():
            epssupp[key] = 1.0*self.epscnt[key]/tcnt
        return epssupp

    def computeASupport(self,minsupp,n = 10):
        """
        Compute alternative supports of episodes in this episode set. 
        """
        epsasupp = {} 
        meansupp = 1.0*self.meanEpsCnt()/self.totalEpsCnt()     
        #max = self.maxEpsCnt().values()[0]
        #min = self.minEpsCnt().values()[0]
        #maxmin   = 1.0*max/min # <max supp>/<min supp>
        epssupp = self.computeSupport() 
        for key in epssupp.keys():
            #epsasupp[key] = epssupp[key] if epssupp[key] >= minsupp else maxmin*meansupp-minsupp 
            supp = epssupp[key]
            isupp = -math.log(supp,2) 
            epsasupp[key] = supp if supp >= minsupp else isupp*meansupp-supp
            #epsasupp[key] = supp if supp >= minsupp else n*meansupp-supp
        return epsasupp
        
        
    def computeWSupport(self,minsupp):
        """
        Compute weighted supports of episodes in this episode set. 
        """
        epswsupp = {}
        meansupp = 1.0*self.meanEpsCnt()/self.totalEpsCnt()
        max = self.maxEpsCnt().values()[0]
        min = self.minEpsCnt().values()[0]
        maxmin   = 1.0*max/min # maxsupp/minsupp
        epssupp = self.computeSupport() 
        
        for key in epssupp.keys():
            supp = epssupp[key] 
            epswsupp[key] = ((maxmin**(meansupp/supp))*abs(supp-minsupp)+1)*supp
        return epswsupp
    
   
    def computeConfidence1(self):
        """
        Compute confidences of episodes in this episode set. 
        """
        epsconf = {} 
        for key in self.epscnt.keys():
            if len(key) == 1:
                epsconf[key] = 1
            else:
                pk = getPrefixByRepr(key)
                epsconf[key] = 1.0*self.epscnt[key]/self.epscnt[pk]
        return epsconf
    
    
    def computeConfidence2(self,epssupp=[]):
        """
        Compute confidences of episodes in the given episodes and their supports.
        """
        epsconf = {} 
        
        osupp = {}
        awsupp = {}

        if len(epssupp) == 1:
           osupp = epssupp[0]
           awsupp = epssupp[0]
        elif len(epssupp) == 2:
           osupp = epssupp[1]
           awsupp = epssupp[0]
        else:
           return epsconf

        for key in awsupp.keys():
            if len(key) == 1:
                epsconf[key] = 1
            #elif awsupp[key] < minsupp:
            #    continue 
            else:
                pk = getPrefixByRepr(key)
                #print osupp[key],osupp[pk]
                epsconf[key] = 1.0*osupp[key]/osupp[pk]
        return epsconf

   
    def prune(self,minsupp,skind):
        """
        Prune episode sets by support threshold.
        """
        awsupp = {}
        osupp  = {}
        tmp1 =  self.computeSupport()
        tmp2 = {} 

        if skind == 1:
           tmp2 = self.computeASupport(minsupp)
        elif skind == 2:
           tmp2 = self.computeWSupport(minsupp)
        else:
           for k in tmp1.keys(): 
              if tmp1[k] >= minsupp:
                 osupp[k] = tmp1[k]         
                 pk = getPrefixByRepr(k) 
                 osupp[pk] = tmp1[pk]         
           del tmp1 
           return [osupp]
        
        for k in tmp2.keys():
           if tmp2[k] >= minsupp:
              awsupp[k] = tmp2[k] 
              osupp[k] = tmp1[k]         
              pk = getPrefixByRepr(k) 
              osupp[pk] = tmp1[pk]         
        del tmp1,tmp2
        return [awsupp,osupp]
    
    ######################################################################
    # Object relative
    ######################################################################
    
    def getEpisodes(self):
        """
        Returns the list of all episodes in this episode set. 
        """
        return self.episodes
    
    def empty(self):
        """
        Checks whether the serial episode is empty?
        """
        return self.length == 0
    
    
    def containsEpisode(self, episoded):
        """
        Checks whether the given episode is in this episode set. 
        """
        return episode in self.episodes

    def createEpsCnt(self):
        """
        Create episode/count for this episode set. 
        """
        self.epscnt.clear()
        for eps in self.episodes:
            if self.epscnt.has_key(eps.repr):
                self.epscnt[eps.repr] += 1
            else:
                self.epscnt[eps.repr] = 1
        self.length = len(self.epscnt)


    ######################################################################
    # Common Util Functions
    ######################################################################

def sign(snum):
    return 1 if snum >= 0 else -1

def getPrefixByRepr(repr):
    """
    Get prefix representation of an episode by it representation.
    """
    if len(repr) > 1:
        lst = repr.split("->")
        del lst[-1] 
        return '->'.join(lst) if len(lst) > 1 else lst[0] 
    else:
        return repr
    
    ######################################################################
    # Main 
    ######################################################################

if __name__ == '__main__':
    epset = EpisodeSet([])
    print getPrefixByRepr("A->B")
    print getPrefixByRepr("A")
    print getPrefixByRepr("A->B->C->D")
    print sign(0.00001)
    print sign(-0.000001)
    print sign(0)
    print sign(-0)
    print sign(10)
    print sign(-10)
    
