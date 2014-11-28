import multiprocessing

import GaudiPython

from Gaudi.Configuration import *
from GaudiConf import IOHelper

from Configurables import CondDB
from Configurables import LHCbApp
from Configurables import DecodeRawEvent
from Configurables import CondDBAccessSvc
from Configurables import SimConf, DigiConf, DstConf


opts = {
    "DDDB":"dddb-20130806",
    "CondDB":"sim-20130722-vc-md100",
    "Others":["VP_Compact_MicroChannel+UT","FT_MonoLayer"],
    "DBpath": "/afs/cern.ch/user/t/thead/public/velo_sim/myDDDB-LHCb-Upgrade-VP-Aug2013.heinrich",
    }

LHCbApp().Simulation = True
CondDB().Upgrade = True
LHCbApp().DDDBtag = opts['DDDB']
LHCbApp().CondDBtag = opts['CondDB']
CondDB().AllLocalTagsByDataType = opts['Others']

ApplicationMgr().ExtSvc += ["ToolSvc", "DataOnDemandSvc"]

myAccessSvc = CondDBAccessSvc("mySuperHyperAccessSvc",
                              ConnectionString="sqlite_file:" + opts['DBpath'] + ".db/DDDB",
                              CacheHighLevel=2000)
CondDB().addLayer(accessSvc=myAccessSvc)

DecodeRawEvent().DataOnDemand = True

# configuration ends here, now starts the execution bit

def event_summaries(fname, N, tests, results):
    """Execute all `tests` on `N` events in `fname`

    For each event a list of the return values of each
    test is send to `results`.
    """
    app_mgr = GaudiPython.AppMgr()

    input = app_mgr.evtsel()
    input.open([fname])
    
    evt = app_mgr.evtsvc()
    # Nasty way of having det available inside
    # test functions without having to pass it
    # explicitly
    global det
    det = app_mgr.detSvc()
    
    for n in xrange(N):
        app_mgr.run(1)
        res = []
        for test in tests:
            res.append(test(evt))

        results.send(res)

    results.send(None)
    results.close()

def run_event_numbers(evt):
    h = evt['/Event/Gen/Header']
    return (h.runNumber(), h.evtNumber())

def number_of_clusters(evt):
    clusters = evt['Raw/VP/LiteClusters']
    return len(clusters)

def N_cluster_channelIDs(N):
    """Return channelIDs for first `N` Velo clusters"""
    def cluster_channelIDs(evt):
        clusters = evt['Raw/VP/LiteClusters']
        clusters = [clusters[n] for n in xrange(N)]
        return [c.channelID().channelID() for c in clusters]

    return cluster_channelIDs

def N_cluster_interpixel_fractions(N):
    def cluster_interpixel_fractions(evt):
        clusters = evt['Raw/VP/LiteClusters']
        clusters = [clusters[n].interPixelFraction() for n in xrange(N)]
        return [(f.first,f.second) for f in clusters]

    return cluster_interpixel_fractions

def channel2point_roundtrip(evt):
    # not sure how useful this is but a good way
    # of remembering how to use the detector elements
    vp = det['/dd/Structure/LHCb/BeforeMagnetRegion/VP']
    clusters = evt['Raw/VP/LiteClusters']

    res = []
    for n in xrange(10):
        cluster = clusters[n]
        sensor = vp.sensorOfChannel(cluster.channelID())
        point = sensor.channelToPoint(cluster.channelID())
        # need to clone it, so make sure it is set to
        # a silly value so we can see if the round trip
        # actually works
        channel_ = cluster.channelID()
        channel_.setChannelID(1124)
        sensor.pointToChannel(point, channel_)
        res.append(channel_.channelID() == cluster.channelID().channelID())
    
    return res
        
def run(before, after):    
    #
    # Configuration is over, start running
    # perform these tests on the two input
    # files and compare the results
    tests = [run_event_numbers,
             number_of_clusters,
             N_cluster_channelIDs(10),
             N_cluster_interpixel_fractions(10),
             channel2point_roundtrip,
             ]

    N = 10

    # first file
    summaries_a, results_a = multiprocessing.Pipe(duplex=False)
    A = multiprocessing.Process(target=event_summaries,
                                args=(before, N, tests, results_a))
    A.start()
    # second file
    summaries_b, results_b = multiprocessing.Pipe(duplex=False)
    B = multiprocessing.Process(target=event_summaries,
                                args=(after, N, tests, results_b))
    B.start()

    summaries = [summaries_a, summaries_b]
    n = 0
    while True:
        sums = [summary.recv() for summary in summaries]

        if all([s is None for s in sums]):
            print "All done"
            break

        if any([s is None for s in sums]):
            print "Whoops, one file is shorter than the others?"
            break

        sums_a, sums_b = sums
        for test, a, b in zip(tests, sums_a, sums_b):
            if not a == b:
                print test.__name__, "not equal in event %d"%(n)
                print a
                print "-"*60
                print b
                print "-"*60

        if any([a!=b for a,b in zip(sums_a, sums_b)]):
            break

        n += 1

    A.join()
    B.join()


if __name__ == "__main__":
    import sys

    if not len(sys.argv) == 3:
        print "usage:",
        print sys.argv[0], "DigiFileA DigiFileB"
        sys.exit(1)
    
    before = sys.argv[1]
    after = sys.argv[2]

    run(before, after)
