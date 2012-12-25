#!/usr/bin/python
    
# Miner & Streamer & MultiStreamer
import sys

import getopt
import os
import time
import logging
import copy

import cProfile
import profile
import threading

from Event import Event
from Window import Window
from Tsbatcher import Tsbatcher
from EpisodeSet import EpisodeSet
from SerialEpisode import SerialEpisode 
    

##########################################################################
# Streamer
# Read events from data stream with a sliding window.
########################################################################## 

class Streamer(threading.Thread):

    ######################################################################
    # Construction & Initialization
    ######################################################################
   
    def __init__(self, name, wl, dataFile):
        '''
        Constructor: 
        name            name of the stream in multiple data stream 
        wl              window length
        dataFile        file containing event stream
        '''
        super(Streamer,self).__init__() 
        
        # Storing the values
        self.name           = name
        self.wl             = wl
        self.dataFile       = dataFile
        
		# Setting switches
        self.window       	= Window(wl)  			# Contents of window
        
    ######################################################################
    # Sliding Window
    ######################################################################
    
    def run(self):
        """
        Read new event data and sliding the window.
        """
        global streamers,miners,conds,windows,nwins

        # Opening the data file
        active = True
        event = Event() 
        #for line in open(self.dataFile,"r"): 
        file = open(self.dataFile)
        # in real application, here should be a buffer to store stream data 
        while True: 
            if conds[self.name].acquire():
              if self.name not in windows.keys():
                 line = file.readline()
                 if len(line) == 0:
                    active = False
                 else: 
                    event = Event(line)	
                    out = self.window.append(event)
                 
                 if len(self.window) < self.wl and active:
                    conds[self.name].release()  
                    continue
                 
                 windows[self.name]  = self.window
                 print "\tStreamer "+self.name+":"+"window is prepared."
                 #print "Streamer "+self.name+":",windows[self.name]
                 nwins[self.name] = nwins[self.name] + 1
                 conds[self.name].notify() 
                 
                 if not active:
                    conds[self.name].release()  
                    break

              conds[self.name].release()  
            else:
              conds[self.name].wait()
        
            time.sleep(1)

        file.close()
        print "\tStreamer "+self.name+":"+"is over." 

#########################################################################
# Miner 
# The mining strart point of each streamer.
######################################################################### 

class Miner(threading.Thread):

    ######################################################################
    # Construction & Initialization
    ######################################################################
    def __init__(self,name,mtp):
        '''
        Constructor: 
        name            name of the miner for stream name in multiple data stream 
        mtp             minimal time span
        '''
        super(Miner,self).__init__() 
        # Storing the values
        self.name           = name
        self.mtp            = mtp  			
    
    ######################################################################
    # Generate Candidate
    ######################################################################
    
    def run(self):
        """
        Read the data in window, get episodes and their statistics information 
        into an episode set. 
        """
        global streamers,miners,conds,windows,win_epssets,nwinm

        epsset = EpisodeSet() 
        while True:
           if conds[self.name].acquire():
              if self.name in windows.keys() and epsset.length == 0:
                 #start       = time.time()
                 tsbatcher   = Tsbatcher(self.mtp,windows[self.name]) 
                 epsset = tsbatcher.episodeStatistic()
                 #end         = time.time()
                 #logging.critical('Time spent mining: %f seconds' % (end - start))
                 del windows[self.name]
                 print "\tMiner "+self.name+":"+"window is done."
                 conds[self.name].notify()  
              elif not streamers[self.name].is_alive():
                 conds[self.name].release()  
                 break 
              
              conds[self.name].release()  
           else:
              conds[self.name].wait()
        
           
           if mutex.acquire():
              if epsset.length != 0 and self.name not in win_epssets.keys(): 
                 win_epssets[self.name]  = copy.deepcopy(epsset)
                 #win_epssets[self.name].__show__()
                 epsset = EpisodeSet() 
                 nwinm[self.name] += 1 
              mutex.release()           

           time.sleep(1)

        print "\tMiner "+self.name+":"+"is over." 
        
    
##########################################################################
# MultiStreamer
# The main class and starting point of the mining. 
##########################################################################

class MultiStreamer:

    ######################################################################
    # Construction & Initialization
    ######################################################################
    
    def __init__(self, dataDir, outputFileName, wl, mtp, alpha, sigma, ratio, skind):
        '''
        Constructor: 
        dataDir         directory containing event stream files
        outputFileName  file in which the output is produced
        wl              window length
        mtp             minimal time span
        alpha           minimal support threshold
        sigma           minimal conference threshold
        ratio           minimal ratio threshold
        skind           kind of support 
        '''
        # Storing the values
        self.dataDir        = dataDir
        self.outputfileName = outputFileName
        self.wl             = wl
        self.mtp            = mtp
        self.alpha			= alpha
        self.sigma          = sigma
        self.ratio          = ratio
        self.skind          = skind
        self.streams        = os.listdir(self.dataDir) 
        
		# Setting switches
        self.time           = 0                             # Current time of the stream
        self.outputFile     = open(outputFileName, "w")     # Output file
    
    
    ######################################################################
    # Mining
    ######################################################################
    
    def mine(self):
        """
        Read events in multiple streams and mining episode through sliding window.
        """
        global streamers,miners,conds,windows,win_epssets,mutex,nwinm,tcnt,wcnt

        # Opening the stream data file directory
        nstream = len(self.streams) 
        if nstream == 0:
            print("No data file in data direcory %s"%(self.dataDir))
            usage()
            sys.exit()
 
        for stream in self.streams:
            conds[stream] = threading.Condition()
            streamers[stream] = Streamer(stream,self.wl,os.path.join(self.dataDir,stream)) 
            miners[stream] = Miner(stream,self.mtp) 
            nwinm[stream] = 0
            nwins[stream] = 0
        
        for stream in self.streams:
            streamers[stream].start()
            miners[stream].start()

        grouped_win_epssets = {} 
        while self.active():
           if self.ready():
              tcnt = tcnt+1 
              if mutex.acquire():
                 print("****Ready to get episode rules.") 
                 #print len(win_epssets)
                 if len(win_epssets) > 1: 
                    grouped_win_epssets = copy.deepcopy(self.group(win_epssets,self.ratio))
                 else:
                    grouped_win_epssets = copy.deepcopy(win_epssets)
                    #print grouped_win_epssets
                 win_epssets.clear()
                 mutex.release()
                 
              self.output(grouped_win_epssets)
           time.sleep(1)

        self.closeOutput()
        for stream in self.streams:
            print "****Number of windows produced by Streamer ["+stream+"] is : ",nwins[stream]
            print "****Number of windows processing by Miner ["+stream+"] is : ",nwinm[stream]
        print "****Total processing times is :",tcnt
        print "****Total write times is :",wcnt
        print("****All stream is inactive!")
        print("****Done !") 

    ######################################################################
    # Utils
    ######################################################################
   
    def group(self,epsst,t):
       """
        Group stream's mining results (episode set of each window) into new episode set.
       """
       length = len(epsst)
       if length == 0:
          return
 
       keys = epsst.keys()
       values = epsst.values()
 
       if length == 2:
           if values[0].commonRatio(values[1]) >= t:
              return self.union({keys[0]:values[0],keys[1]:values[1]})
           else:
              return epsst
       
       flags = [-1,-1]
       maxrt = 0.0 
 
       Unioned = {} 
       for i in range(0,length):
           for j in range(0,length):
               if j == i:
                  break
               else:
                  rt = values[i].commonRatio(values[j])
                  if rt >= t and rt >= maxrt :
                     maxrt = rt 
                     flags[0] = i
                     flags[1] = j
  
       if flags[0] == -1:
           return epsst
 
       Unioned = self.union({keys[flags[0]]:values[flags[0]],keys[flags[1]]:values[flags[1]]}) 
       #print Unioned 
 
       for i in range(0,length):
           if i != flags[0] and i != flags[1]:
               Unioned[keys[i]] = values[i]
 
       return self.group(Unioned,t)

    def union(self,epsdic):
        """
         Union epsisode sets in a dictionary, key is stream's name and value is episode set.
         Return new dictionary. 
        """
        lh = len(epsdic)
        ks = epsdic.keys() 
        vs = epsdic.values() 
        k = ks[0]
        v = vs[0]
        for i in range(1,lh):
            k += ","+ks[i]
            v.merge(vs[i])
        return {k:v}
  

    def active(self):
       """
        Check if there is any stream active. 
       """
       active = False
       for k in miners.keys():
         if miners[k].is_alive():
            active = True
         else:
            del miners[k] 
            del streamers[k] 
            del conds[k] 
       return active
    
    def ready(self):
       """
        Check if it is ready to get final result of this period.
       """
       return len(win_epssets) >= len(miners)
    
    ######################################################################
    # Outputting
    ######################################################################
    def output(self,epsst):
        """
        Produces output and writes it to the disk. 
        """
        # Logging: nice for progress monitoring
        #logging.info("%d: %s" % (self.(), "output"))
        
        # For each stream, get the result of the current window
        # Write!
        global wcnt
        wcnt = wcnt+1 

        sptstr = {0:'support',1:'alternative support',2:'weighted support'} 
        lnsp = ['#']*100
        lnsp.append('\n')
        lnstr = ''.join(lnsp)
        
        self.outputFile.write(lnstr)
        self.outputFile.write("#\t%d\n"%(wcnt))
        self.outputFile.write(lnstr)
        print("****Write current mining result to "+self.outputfileName) 
        for key in epsst.keys():
            self.outputFile.write(lnstr)
            self.outputFile.write("#\t%s\n"%(key))
            self.outputFile.write(lnstr)
            
            epssupp = epsst[key].prune(self.alpha,self.skind) 
            epsconf = epsst[key].computeConfidence1()
            epsconf = epsst[key].computeConfidence2(epssupp)
            
            if len(epssupp) > 1:
                self.outputFile.write("#%20s,%20s,%20s,%20s\n"%("episode",sptstr[0],sptstr[self.skind],"confidence"))
                self.outputFile.write(lnstr)
            else:
                self.outputFile.write("#%20s,%20s,%20s\n"%("episode",sptstr[self.skind],"confidence"))
                self.outputFile.write(lnstr)
            
            for k in epssupp[0].keys():
                if len(k) == 1:
                    continue
                if epssupp[0][k] >= self.alpha and epsconf[k] >= self.sigma:
                    if len(epssupp) > 1:
                       self.outputFile.write("%20s,%20s,%20s,%20s\n"%(k,epssupp[1][k],epssupp[0][k],epsconf[k]))
                    else:
                       self.outputFile.write("%20s,%20s,%20s\n"%(k,epssupp[0][k],epsconf[k]))
        self.outputFile.write(lnstr)
        self.outputFile.write("\n\n\n")
        
    def closeOutput(self):
        """
        Closes all the open output files
        """
        self.outputFile.close()

########################################################################
# Starting Mining!
########################################################################

# global variables

conds       =   {}
windows     =   {} 
win_epssets =   {} 
streamers   =   {}
miners      =   {}
nwinm       =   {}
nwins       =   {}
mutex       =   threading.Lock()
tcnt        =   0 
wcnt        =   0 

def usage():
    print("Usage: python MultiStreamer.py -i <directory> -o <file> [-w <integer>]" \
            "[-t <integer>] [-a <float>] [-s <float>] [-r <float>]")
    print("-i   the directory containing the event stream files")
    print("-o   the file to which the output is written")
    print("-w   setting window length (default = 20)")
    print("-t   setting minimal time span of episode (default = 4)")
    print("-a   setting minimal support threshold [0-1] (default = 0.02)")
    print("-s   setting minimal conference threshold [0-1] (default = 0.5)")
    print("-r   setting minimal ratio threshold for grouping multiple streams [0-1] (default = 0.3)")
    print("-k   setting kind of support for mining [{0:suport, 1:alternative support,2 weighted support} (default = 0)")


if __name__ == '__main__':
    # Parsing the arguments
    dataDir         = None
    outputFile      = None
    wl              = 20
    mtp             = 4
    alpha           = 0.02
    sigma           = 0.5
    ratio           = 0.3
    skind           = 0
    
    
    opts, _ = getopt.getopt(sys.argv[1:], "hi:o:w:t:k:s:a:r:")
    for arg, v in opts:
        if arg == "-i":
            dataDir = v
        elif arg == "-h":     
			usage()
			sys.exit()
        elif arg == "-o":
            outputFile = v
        elif arg == "-w":
            wl = int(v)
        elif arg == "-t":
            mtp = int(v)
        elif arg == "-a":
            alpha = float(v)
        elif arg == "-s":
            sigma = float(v)
        elif arg == "-r":
            ratio = float(v)
        elif arg == "-k":
            skind = float(v)
        else:
			usage()
			sys.exit()

    
    # Checking the parameters
    if dataDir  == None or not os.path.exists(dataDir):
        print("No data directory!")
        usage()
        sys.exit()
    
    if outputFile == None:
        print("Output directory must exist")
        usage()
        sys.exit()
    
    if wl < 1:
        print("Window cannot have a negative length!")
        usage()
        sys.exit()
	
	if mtp < 1:
	    print("Minmal time span cannot have a negative length!")
	    usage()
        sys.exit()
	
    if not(0 <= alpha <= 1):
         print("Minimal support threshold has to be between 0 and 1")
         usage()
         sys.exit()
    
    if not(0 <= sigma <= 1):
         print("Minimal conference threshold has to be between 0 and 1")
         usage()
         sys.exit()
    
    if not(0 <= ratio <= 1):
         print("Minimal multiple streams ratio threshold has to be between 0 and 1")
         usage()
         sys.exit()
    
    if not (skind == 0 or skind == 1 or skind == 2):
         print("Kind of support has to be 0, 1 or 2 for support, alternative support and weighted support")
         usage()
         sys.exit()
    
    # Start the mining!
    print("****Start mining!")
    mstreamer = MultiStreamer(dataDir, outputFile, wl, mtp, alpha, sigma, ratio, skind)
    mstreamer.mine()
    
