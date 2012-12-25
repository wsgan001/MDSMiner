#!/usr/bin/python
# 
# MEGenerator
# Class to generate synthetic data set to simulate multple data streams.
# 

import ConfigParser    
import string, os, sys    
#import copy
#import functools
import random

def pick_by_pb(_list,pb):
   x=random.uniform(0,1)
   cumulative_pb = 0.0
   for item, item_pb in zip(_list,pb):
      cumulative_pb += float(item_pb)
      if x < cumulative_pb : break
   return item

# data set generator 
def genData(etype,ts,nd,file,pbmods):
  event_types = etype.split(',')  
  pbmode= pbmods.split(',')  
  t = 0
  #t = random.randint(t+1,t+ts)
  of = open(file,'w')
  i = 1
  while i <= nd:
     t = random.randint(t+1,t+ts)
     #of.write("%s,%d\n"%(random.choice(event_types),t))
     of.write("%s,%d\n"%(pick_by_bp(event_types,pbmode),t))
     i += 1
  of.close()


if __name__ == '__main__':
  # config parser
  cf = ConfigParser.ConfigParser()    
  cf.read("gen.conf")    
  #s = cf.sections()
  #print 'section:', s   
  #section: ['global', 'local'] 
  #o = cf.options("global")    
  #print 'options:', o    
  #v = cf.items("global")    
  #print 'db:', v
  # number of streams 
  ns = cf.getint('global','ns') 
  # maximum time span of episode
  ts = cf.getint('global','ts') 
  # number of data points in each synthetic stream
  nd = cf.getint('global','nd') 
  # prefix of output file name 
  pf = cf.get('global','pf')
  # output directory 
  od = cf.get('global','od')
  if not os.path.exists(od):
     os.mkdir(od)
  etypes = []
  pbmods = []
  for i in range(ns):
     etypes.insert(i,cf.get('local','etype%d'%(i+1)))
     pbmods.insert(i,cf.get('local','pbmod%d'%(i+1))) 
     genData(etypes[i],ts,nd,"%s%s%s_%d"%(od,os.sep,pf,i+1),pbmods[i]) 


