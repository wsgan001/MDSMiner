#!/usr/bin/python
#
# SingleStreamer
# The main class and starting point of the mining. 
# 

import sys

import getopt
import os
import time
import logging
import copy

import cProfile
import profile

from Event import Event
from Window import Window
from Tsbatcher import Tsbatcher
from EpisodeSet import EpisodeSet

class SingleStreamer:

    ######################################################################
    # Construction & Initialization
    ######################################################################
    def __init__(self, dataFile, outputFileName, wl, mtp, alpha, sigma, printSummary):
        '''
        Constructor: 
        dataFile        file containing event stream
        outputFileName  file in which the output is produced
        wl              window length
        mtp             minimal time span
        alpha           minimal support threshold
        sigma           minimal conference threshold
        printSummary    print the summaries at the print interval
        '''
        # Storing the values
        self.dataFile       = dataFile
        self.outputfileName = outputFileName
        self.wl             = wl
        self.mtp            = mtp
        self.alpha			= alpha
        self.sigma          = sigma
        self.printSummary   = printSummary
        
		# Setting switches
        self.time           = 0                             # Current time of the stream
        self.outputFile     = open(outputFileName, "w")     # Output file
        self.window       	= Window(wl)         			# Contents of window
    
    ######################################################################
    # Mining
    ######################################################################
    def mine(self):
        """
        Read the itemsets, feed them into the summaries and prune the summaries.
        At certain times, print out the minimal window and the max-frequency of 
        all the itemsets. 
        """
		# Opening the data file
        data = open(self.dataFile)
        
        # Processing one itemset at the time
        start = time.time()
        #print 'tcnt,lsupp,uasupp,uwsupp,uasuppc,uwsuppc'
        for line in data:
            # Setting the time one further
            event = Event(line)	
            self.time = event.time
            out = self.window.append(event)
            if len(self.window) < self.wl:
			    continue
            tsbatcher = Tsbatcher(self.mtp,self.window) 
    
            #print self.window
            #self.epset.merge(tsbatcher.EpsCnt())
            #ms = 0.02
            #mc = 0.6
            #epset      = tsbatcher.episodeStatistic()
            #support    = epset.computeSupport() 
            #asupport   = epset.computeASupport(ms) 
            #wsupport   = epset.computeWSupport(ms) 
            #confidence = epset.computeConfidence() 
             
            #print epset.__show__()
            #tcnt  = 0
            #lsupp = 0
            #uasupp = 0
            #uwsupp = 0
            #uasuppc = 0
            #uwsuppc = 0
            
            #for key in support.keys():
            #     tcnt += 1 
            #     if support[key] < ms:
            #        lsupp += 1 
            #        if asupport[key] >= ms:
            #            uasupp += 1 
            #            if confidence[key] >= mc:
            #               uasuppc += 1 
            #        if wsupport[key] >= ms:
            #           uwsupp += 1 
            #           if confidence[key] >= mc:
            #              uwsuppc += 1 

            #print "%-5d%-6d%-7d%-7d%-8d%-8d"%(tcnt,lsupp,uasupp,uwsupp,uasuppc,uwsuppc)
            
            #print "%-12s|%-12s|%-12s|%-12s|%-12s"%("key","support","asupport","wsupport","confidence")
            #for key in support.keys():
            #    if support[key] < 0.02 and (asupport[key] >= 0.02 or wsupport[key] >= 0.02) and confidence[key] < 0.4:
            #        print "%-12s|%-12s|%-12s|%-12s|%-12s"%(key,support[key],asupport[key],wsupport[key],confidence[key])
           
            #print "#######################################################################################################"
            #print "%-12s|%-12s|%-12s|%-12s|%-12s"%("key","support","asupport","wsupport","confidence")
            #for key in support.keys():
            #    if support[key] < 0.02 and (asupport[key] >= 0.02 or wsupport[key] >= 0.02) and confidence[key] >= 0.4:
            #        print "%-12s|%-12s|%-12s|%-12s|%-12s"%(key,support[key],asupport[key],wsupport[key],confidence[key])
            
            #epset.__show__()
        
            #cnt = self.window.getCount(event)
            #print self.window[-1] 
            #print out,cnt
            # Check whether we need to produce output
            self.output()
            
        #self.epset.__show__()
        end = time.time()
        logging.critical('Time spent mining: %f seconds' % (end - start))
        
        # Closing the data file
        self.closeOutput()
        data.close()
    
    
    ######################################################################
    # Getters
    ######################################################################
    def getTime(self):
        """
        Returns the current time (for the summary). Thus if there is a minimal 
        window, the time is the time of the stream minus the length of the 
        minimal window, unless we're in the new algorithm in which case the
        summary is up to date. 
        """
        if self.wl == 1:
            return self.time
        else:
            return self.time - self.wl
    
    
    def getStreamTime(self):
        """
        Returns the current time of the stream, i.e. at the end, not before
        the minimal window. 
        """
        return self.time
    
    
    def getWindow(self):
        """
        Returns the window. 
        """
        return self.window
    
    
    
    ######################################################################
    # Outputting
    ######################################################################
    def output(self):
        """
        Produces output and writes it to the disk. 
        """
        # Logging: nice for progress monitoring
        logging.info("%d: %s" % (self.getStreamTime(), "output"))
        
        # For each summary, ask the maximal window
        # Write!
        self.outputFile.write(
                "%s\n" % (str("test itemset")))
            #    "%s\t%d\t%d\t%f\n" % (str(itemset), self.getStreamTime(), start, maxFreq))
        
    def closeOutput(self):
        """
        Closes all the open output files
        """
        self.outputFile.close()
        
        if self.printSummary:
            self.summaryOut.close()
    
    
    
######################################################################
# Start Mining!
######################################################################
def usage():
    print("Usage: python Streamer.py -i <file> -o <file> [-w <integer>]" \
            "[-t <integer>] [-a <float>] [-s <float>] ")
    print("-i   the file containing the event stream")
    print("-o   the file to which the output is written")
    print("-w   setting the window length (default = 20)")
    print("-t   setting the minimal time span of episode (default = 4)")
    print("-a   setting the minimal support threshold [0-1] (default = 0.02)")
    print("-s   setting the minimal conference threshold [0-1] (default = 0.5)")



if __name__ == '__main__':
    # Parsing the arguments
    dataFile        = None
    outputFile      = None
    wl              = 20
    mtp             = 4
    alpha           = 0.02
    sigma           = 0.5
    printSummary    = False
    
    opts, _ = getopt.getopt(sys.argv[1:], "hi:o:w:t:s:", ["summary"])
    for arg, v in opts:
        if arg == "-i":
            dataFile = v
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
        elif arg == "--summary":
            printSummary = True
        else:
			usage()
			sys.exit()

    
    # Checking the parameters
    if dataFile == None or not os.path.exists(dataFile):
        print("No data!")
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
         print("The minimal support threshold has to be between 0 and 1")
         usage()
         sys.exit()
    
    if not(0 <= sigma <= 1):
         print("The minimal conference threshold has to be between 0 and 1")
         usage()
         sys.exit()
    
    # Start the mining!
    #print("Start mining!")
    streamer = SingleStreamer(dataFile, outputFile, wl, mtp, alpha, sigma, printSummary)
    streamer.mine()
    
    #cProfile.run('streamer.mine()', 'out.profile')
    
