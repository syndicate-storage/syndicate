#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
"""

"""
This code is for debugging purposes only.
It will be removed in the production version of the MS.
DO NOT RELY ON IT IN THE LONG TERM
"""

import MS
import storage.storage as storage
from MS.entry import *
from MS.gateway import *
from google.appengine.ext import ndb
import traceback

# a bunch of (mostly) active planetlab nodes, for which UGs will be created.
nodes = [
   "lsirextpc01.epfl.ch",
   "planetlab2.csg.uzh.ch",
   "ple2.ipv6.lip6.fr",
   "planetlab2.ionio.gr",
   "ple01.fc.univie.ac.at",
   "pnode1.pdcc-ntu.singaren.net.sg",
   "onelab3.info.ucl.ac.be",
   "planetlab1.xeno.cl.cam.ac.uk",
   "peeramide.irisa.fr",
   "planetlab01.tkn.tu-berlin.de",
   "planetlab2.umassd.edu",
   "planetlab4.wail.wisc.edu",
   "planetlab2.koganei.wide.ad.jp",
   "planetlab-2.cmcl.cs.cmu.edu",
   "planetlab-13.e5.ijs.si",
   "planetlab1.cs.uoregon.edu",
   "planetlab-coffee.ait.ie",
   "ple2.tu.koszalin.pl",
   "planetlab-3.cs.ucy.ac.cy",
   "planetlab1.sics.se",
   "planet-lab3.uba.ar",
   "planet-lab4.uba.ar",
   "planetlab1.lkn.ei.tum.de",
   "aguila2.lsi.upc.edu",
   "planck249ple.test.iminds.be",
   "planck250ple.test.iminds.be",
   "planetlab3.di.unito.it",
   "planetlab-02.kusa.ac.jp",
   "csplanetlab3.kaist.ac.kr",
   "aguila1.lsi.upc.edu",
   "planetlab1.cs.uiuc.edu",
   "planetlab7.millennium.berkeley.edu",
   "earth.cs.brown.edu",
   "vn4.cse.wustl.edu",
   "roam2.cs.ou.edu",
   "pl1.rcc.uottawa.ca",
   "planetlab03.cs.washington.edu",
   "plab1.nec-labs.com",
   "pli1-pa-6.hpl.hp.com",
   "planet-plc-3.mpi-sws.org",
   "planetlab3.singaren.net.sg",
   "planetlab4.postel.org",
   "planetlab1.iitkgp.ac.in",
   "planetlab1.poly.edu",
   "planetlab-03.cs.princeton.edu",
   "ricepl-2.cs.rice.edu",
   "planetlab2-tijuana.lan.redclara.net",
   "pli2-pa-2.hpl.hp.com",
   "planetlab2.comp.nus.edu.sg",
   "planetlab2.buaa.edu.cn",
   "node1.planetlab.albany.edu",
   "planet1.l3s.uni-hannover.de",
   "planetlab1.eee.hku.hk",
   "planetlab6.flux.utah.edu",
   "planetlab1.cs.otago.ac.nz",
   "75-130-96-12.static.oxfr.ma.charter.com",
   "planetlab1.singaren.net.sg",
   "pl1.eng.monash.edu.au",
   "planetlab-2.sysu.edu.cn",
   "planetlab1.pop-pa.rnp.br",
   "node1.planetlab.mathcs.emory.edu",
   "planet-lab2.itba.edu.ar",
   "planetlab2.dit.upm.es",
   "plab-2.diegm.uniud.it",
   "planetvs2.informatik.uni-stuttgart.de",
   "node2pl.planet-lab.telecom-lille1.eu",
   "flow.colgate.edu",
   "75-130-96-13.static.oxfr.ma.charter.com",
   "planetlab2.upm.ro",
   "planetlab1.tmit.bme.hu",
   "planetlab2.plab.ege.edu.tr",
   "plab-1.diegm.uniud.it",
   "planetlab-2.fokus.fraunhofer.de",
   "peeramidion.irisa.fr",
   "planetlab1.aut.ac.nz",
   "planetlab1.plab.ege.edu.tr",
   "planetlab-um00.di.uminho.pt",
   "iason.inf.uth.gr",
   "planetlab2.tau.ac.il",
   "dannan.disy.inf.uni-konstanz.de",
   "host2.planetlab.informatik.tu-darmstadt.de",
   "147-179.surfsnel.dsl.internl.net",
   "plab4.ple.silweb.pl",
   "planetlab1.koganei.wide.ad.jp",
   "planetlab6.cs.duke.edu",
   "planetlab02.dis.unina.it",
   "planetlab-1.imag.fr",
   "planetlab2.cs.pitt.edu",
   "planetlab-1.sjtu.edu.cn",
   "planetlab2.bgu.ac.il",
   "node2.planetlab.mathcs.emory.edu",
   "planetlab6.cs.cornell.edu",
   "planetlab1.informatik.uni-goettingen.de",
   "planet2.l3s.uni-hannover.de",
   "planet-plc-2.mpi-sws.org",
   "planetlab1-tijuana.lan.redclara.net",
   "pl4.cs.unm.edu",
   "planetlab2.fct.ualg.pt",
   "planetlab-3.iscte.pt",
   "planetlab1.ionio.gr",
   "planet-plc-4.mpi-sws.org",
   "planetlab1lannion.elibel.tm.fr",
   "planetlab2.science.unitn.it",
   "uoepl1.essex.ac.uk",
   "planetlab2.unineuchatel.ch",
   "planetlab4.cslab.ece.ntua.gr",
   "plab3.ple.silweb.pl",
   "planetlab1.ifi.uio.no",
   "planetlab2.ifi.uio.no",
   "planetlab2.pop-mg.rnp.br",
   "planetlab2.eurecom.fr",
   "planetlab4.hiit.fi",
   "planetlab4.cs.st-andrews.ac.uk",
   "planetlab2.tmit.bme.hu",
   "planetlab-1.man.poznan.pl",
   "planetlab2.lkn.ei.tum.de",
   "planetlab02.cs.tcd.ie",
   "planetlab2.mta.ac.il",
   "planetlab1.cyfronet.pl",
   "planetlab02.cnds.unibe.ch",
   "nis-planet1.doshisha.ac.jp",
   "planet1.unipr.it",
   "plewifi.ipv6.lip6.fr",
   "ple2.dmcs.p.lodz.pl",
   "pandora.we.po.opole.pl",
   "planetlab-02.bu.edu",
   "lim-planetlab-1.univ-reunion.fr",
   "planet2.servers.ua.pt",
   "thalescom-48-42.cnt.nerim.net",
   "dschinni.planetlab.extranet.uni-passau.de",
   "planetlab1.aston.ac.uk",
   "pli2-pa-3.hpl.hp.com",
   "planet-lab-node2.netgroup.uniroma2.it",
   "inriarennes2.irisa.fr",
   "planet1.servers.ua.pt",
   "host1.planetlab.informatik.tu-darmstadt.de",
   "ple3.ipv6.lip6.fr",
   "planetlab4.williams.edu",
   "ple4.ipv6.lip6.fr",
   "planetlab2.cesnet.cz",
   "iraplab1.iralab.uni-karlsruhe.de",
   "planetlab1.exp-math.uni-essen.de",
   "planetlab3.hiit.fi",
   "146-179.surfsnel.dsl.internl.net",
   "evghu4.colbud.hu",
   "host149-82-static.93-94-b.business.telecomitalia.it",
   "evghu5.colbud.hu",
   "evghu6.colbud.hu",
   "evghu7.colbud.hu",
   "planetlab2.unl.edu",
   "planetlab-1.research.netlab.hut.fi",
   "planetlab1.ics.forth.gr",
   "planetlab04.cnds.unibe.ch",
   "planetlab2.s3.kth.se",
   "ple02.fc.univie.ac.at",
   "planetlab-1.iscte.pt",
   "planetlab2.ci.pwr.wroc.pl",
   "planetlab3.eecs.umich.edu",
   "planetlab01.cs.tcd.ie",
   "mercury.silicon-valley.ru",
   "planetlab5.csee.usf.edu",
   "planet3.cs.huji.ac.il",
   "planetlab2.diku.dk",
   "planetlab2.byu.edu",
   "planetlab-4.imperial.ac.uk",
   "planetlab2.cs.uit.no",
   "planetlab-4.cs.ucy.ac.cy",
   "planetlab-12.e5.ijs.si",
   "ple6.ipv6.lip6.fr",
   "planetlab1.fct.ualg.pt",
   "iraplab2.iralab.uni-karlsruhe.de",
   "host147-82-static.93-94-b.business.telecomitalia.it",
   "planetlab-1.cs.ucy.ac.cy",
   "planetlab2.informatik.uni-goettingen.de",
   "planet01.hhi.fraunhofer.de",
   "planetlab-1.ing.unimo.it",
   "orval.infonet.fundp.ac.be",
   "plab2-itec.uni-klu.ac.at",
   "planetlab2.thlab.net",
   "planetlab02.tkn.tu-berlin.de",
   "planetlab1.willab.fi",
   "planetlab2.willab.fi",
   "planet1.inf.tu-dresden.de",
   "plab1.tidprojects.com",
   "ait05.us.es",
   "planetlab2.urv.cat",
   "onelab-2.fhi-fokus.de",
   "thalescom-48-41.cnt.nerim.net",
   "plab1.cs.msu.ru",
   "planetlab2.research.nicta.com.au",
   "prata.mimuw.edu.pl",
   "ple1.dmcs.p.lodz.pl",
   "planetlab1.upm.ro",
   "planetlab1.di.unito.it",
   "plab2.engr.sjsu.edu",
   "pl001.ece.upatras.gr",
   "plab2.tidprojects.com",
   "planet6.cs.ucsb.edu",
   "planetlab1.rutgers.edu",
   "planetlabeu-1.tssg.org",
   "planetlab2lannion.elibel.tm.fr",
   "planetlab1.ci.pwr.wroc.pl",
   "lsirextpc02.epfl.ch",
   "planetlab1.rd.tut.fi",
   "planetlab2.rd.tut.fi",
   "planetlab1.uc3m.es",
   "chimay.infonet.fundp.ac.be",
   "planetlab1.esprit-tn.com",
   "venus.silicon-valley.ru",
   "planetlab1.cs.ucla.edu",
   "planetlab-2.cs.unibas.ch",
   "planetlab2.polito.it",
   "planetlab2.netlab.uky.edu",
   "planetlab2.uc3m.es",
   "planetlab2.utt.fr",
   "planetlabpc1.upf.edu",
   "planetlab2.esprit-tn.com",
   "onelab4.warsaw.rd.tp.pl",
   "planetlab1.upc.es",
   "onelab7.iet.unipi.it",
   "planetlabeu-2.tssg.org",
   "planetlab3.upc.es",
   "planetlab1.eecs.jacobs-university.de",
   "planetlab2.eecs.jacobs-university.de",
   "planetlab-2.ing.unimo.it",
   "planet2.unipr.it",
   "planetlab1.informatik.uni-erlangen.de",
   "pl002.ece.upatras.gr",
   "planetlab-node1.it-sudparis.eu",
   "gschembra3.diit.unict.it",
   "ple5.ipv6.lip6.fr",
   "prometeusz.we.po.opole.pl",
   "planetlab-node3.it-sudparis.eu",
   "planetlab-1.fokus.fraunhofer.de",
   "planetlab2.di.unito.it",
   "planetlab-2.iscte.pt",
   "planetlab-1.cs.unibas.ch",
   "ple2.cesnet.cz",
   "planetlab2.di.fct.unl.pt",
   "evghu13.colbud.hu",
   "evghu14.colbud.hu",
   "planetlab1.itsec.rwth-aachen.de",
   "planetlab2.itsec.rwth-aachen.de",
   "planck227ple.test.ibbt.be",
   "ple1.cesnet.cz",
   "planetlab3.williams.edu",
   "planetlab1.buaa.edu.cn",
   "planetlab1.urv.cat",
   "planetlab1.tlm.unavarra.es",
   "planetlab2.tlm.unavarra.es",
   "planet1.elte.hu",
   "planet-plc-6.mpi-sws.org",
   "planet3.cc.gt.atl.ga.us",
   "planetlab5.eecs.umich.edu",
   "planetlab1.cs.unc.edu",
   "csplanetlab4.kaist.ac.kr",
   "ttu1-1.nodes.planet-lab.org",
   "pl02.comp.polyu.edu.hk",
   "planetlab2.cs.cornell.edu",
   "planetlab1.cesnet.cz",
   "pli1-pa-5.hpl.hp.com",
   "planetlab4.tamu.edu",
   "planetlab4.inf.ethz.ch",
   "planetlab-1.usask.ca",
   "planetlab5.millennium.berkeley.edu",
   "planetlab-n2.wand.net.nz",
   "planetlab1.u-strasbg.fr",
   "planetlab2.u-strasbg.fr",
   "planetlab-2.ssvl.kth.se",
   "planetlab05.mpi-sws.mpg.de",
   "planet-lab1.cs.ucr.edu",
   "planetlab-2.cs.uh.edu",
   "planetlab-01.bu.edu",
   "pli1-pa-4.hpl.hp.com",
   "plnode-04.gpolab.bbn.com",
   "planet5.cs.ucsb.edu",
   "planet-lab-node1.netgroup.uniroma2.it",
   "planet1.cc.gt.atl.ga.us",
   "planetlab1.c3sl.ufpr.br",
   "planetlab3.cs.uoregon.edu",
   "planetlab4.mini.pw.edu.pl",
   "plab2.larc.usp.br",
   "ricepl-4.cs.rice.edu",
   "planetlab2.nakao-lab.org",
   "planet2.cc.gt.atl.ga.us",
   "planetlab6.csee.usf.edu",
   "planetlab1.cs.ubc.ca",
   "planetlab1.byu.edu",
   "planetlab1-buenosaires.lan.redclara.net",
   "pluto.cs.brown.edu",
   "pli2-pa-1.hpl.hp.com",
   "mtuplanetlab2.cs.mtu.edu",
   "planetlab1.tamu.edu",
   "pnode2.pdcc-ntu.singaren.net.sg",
   "plgmu2.ite.gmu.edu",
   "planet2.pnl.nitech.ac.jp",
   "planet1.pnl.nitech.ac.jp",
   "planetlab-3.imperial.ac.uk",
   "ttu2-1.nodes.planet-lab.org",
   "planetlab2.inf.ethz.ch",
   "evghu9.colbud.hu",
   "planetlab-1.calpoly-netlab.net",
   "mars.planetlab.haw-hamburg.de",
   "planet4.cc.gt.atl.ga.us",
   "planetlab1.inf.ethz.ch",
   "planetlab1.millennium.berkeley.edu",
   "planetlab-1.sysu.edu.cn",
   "planet-plc-1.mpi-sws.org",
   "planetlab2.acis.ufl.edu",
   "planetlab4.cs.uoregon.edu",
   "planetlab3.tamu.edu",
   "planetlab-01.cs.princeton.edu",
   "pl2.cs.unm.edu",
   "planetlab1.cs.umass.edu",
   "planetlab1.csg.uzh.ch",
   "planetlab4.n.info.eng.osaka-cu.ac.jp",
   "planetlab4.canterbury.ac.nz",
   "planetlab4.rutgers.edu",
   "planetlab1.temple.edu",
   "planetlab-03.vt.nodes.planet-lab.org",
   "planetlab3.eecs.northwestern.edu",
   "planetlab5.csail.mit.edu",
   "pl1.planetlab.ics.tut.ac.jp",
   "vn5.cse.wustl.edu",
   "plonk.cs.uwaterloo.ca",
   "planetlab1.dojima.wide.ad.jp",
   "planetlab1.cs.cornell.edu",
   "planetlab-n1.wand.net.nz",
   "planetlab1.comp.nus.edu.sg",
   "pl1.pku.edu.cn",
   "planet-plc-5.mpi-sws.org",
   "planetlab2.cti.espol.edu.ec",
   "ebb.colgate.edu",
   "planetlab1.diku.dk",
   "onelab-1.fhi-fokus.de",
   "planetlab2.millennium.berkeley.edu",
   "jupiter.cs.brown.edu",
   "planetlab12.millennium.berkeley.edu",
   "planetlab1.cnds.jhu.edu",
   "plab1.cs.ust.hk",
   "pl2.pku.edu.cn",
   "planetlab1.pop-rs.rnp.br",
   "planetlab-2.calpoly-netlab.net",
   "planetlab1.informatik.uni-leipzig.de",
   "planetlab1.unl.edu",
   "planetlab1.mnlab.cti.depaul.edu",
   "planetlab2.cs.umass.edu",
   "planetlab2.mnlab.cti.depaul.edu",
   "planetlab1.bgu.ac.il",
   "pl1snu.koren.kr",
   "planetlab2.iis.sinica.edu.tw",
   "planet-lab2.cs.ucr.edu",
   "planetlab1.pop-mg.rnp.br",
   "planetlab-2.usask.ca",
   "planetlab2.informatik.uni-leipzig.de",
   "stella.planetlab.ntua.gr",
   "planetlab-2.research.netlab.hut.fi",
   "planetlab1.pjwstk.edu.pl",
   "planetlab3.cs.st-andrews.ac.uk",
   "planet1.cs.huji.ac.il",
   "plab1-itec.uni-klu.ac.at",
   "onelab6.iet.unipi.it",
   "chronos.disy.inf.uni-konstanz.de",
   "planetlab2.cs.aueb.gr",
   "planetlab1.eecs.umich.edu",
   "planetlab1.postel.org",
   "planetlab2.cyfronet.pl",
   "planetlab04.cs.washington.edu",
   "ple2.ait.ac.th",
   "planetlab7.flux.utah.edu",
   "planetlabpc2.upf.edu",
   "plab1.create-net.org",
   "pln.zju.edu.cn",
   "planetlab2.cis.upenn.edu",
   "planetlab1.um.es",
   "planetlab2.um.es",
   "pl-node-1.csl.sri.com",
   "planetlab2.aston.ac.uk",
   "planetlab1.science.unitn.it",
   "planetlab-2.man.poznan.pl",
   "planetlab-2.imperial.ac.uk",
   "onelab3.warsaw.rd.tp.pl",
   "planetlab1.cs.uit.no",
   "planet4.cs.ucsb.edu",
   "planetlab2.postel.org",
   "planetlab1.tau.ac.il",
   "planetlab2.cnds.jhu.edu",
   "planetlab1.iis.sinica.edu.tw",
   "planetlab3.cs.columbia.edu",
   "planetlab-3.cmcl.cs.cmu.edu",
   "planetlab2.ucsd.edu",
   "planetlab2.williams.edu",
   "planetlab-1.cs.colostate.edu",
   "planetlab-tea.ait.ie",
   "planetlab-3.dis.uniroma1.it",
   "evghu11.colbud.hu",
   "evghu12.colbud.hu",
   "planetlab8.millennium.berkeley.edu",
   "planetlab1.georgetown.edu",
   "pl1.yonsei.ac.kr",
   "pl2.yonsei.ac.kr",
   "planetlab2.uta.edu",
   "planetlab1.cs.okstate.edu",
   "kupl2.ittc.ku.edu",
   "planetlab2.wiwi.hu-berlin.de",
   "pl1.bell-labs.fr",
   "pl2.bell-labs.fr",
   "planetlab-1.tagus.ist.utl.pt",
   "node1pl.planet-lab.telecom-lille1.eu",
   "itchy.comlab.bth.se",
   "scratchy.comlab.bth.se",
   "planetlab2.xeno.cl.cam.ac.uk",
   "planetlab-01.ece.uprm.edu",
   "planetlab6.csail.mit.edu",
   "plab2.cs.ust.hk",
   "planetlab-1.cs.auckland.ac.nz",
   "planetlab4.csail.mit.edu",
   "planetvs1.informatik.uni-stuttgart.de",
   "planetlab1.acis.ufl.edu",
   "planetslug5.cse.ucsc.edu",
   "planetlab6.goto.info.waseda.ac.jp",
   "planetlab2.jhu.edu",
   "planetlab0.otemachi.wide.ad.jp",
   "host3-plb.loria.fr",
   "plab2.ple.silweb.pl",
   "planetlab1.eurecom.fr",
   "planetlab2.informatik.uni-wuerzburg.de",
   "planetlab02.ethz.ch",
   "planetlab2.aut.ac.nz",
   "planet2.colbud.hu",
   "inriarennes1.irisa.fr",
   "plab2.create-net.org",
   "planetlabtwo.ccs.neu.edu",
   "pl2.eecs.utk.edu",
   "planetlab1.research.nicta.com.au",
   "host4-plb.loria.fr",
   "planetlab01.sys.virginia.edu",
   "planetlab3.xeno.cl.cam.ac.uk",
   "planetlab2.eecs.umich.edu",
   "plab1-c703.uibk.ac.at",
   "planet2.zib.de",
   "planck228ple.test.ibbt.be",
   "ops.ii.uam.es",
   "planetlab-4.eecs.cwru.edu",
   "planetlab1.nrl.eecs.qmul.ac.uk",
   "plab3.cs.msu.ru",
   "planetlab2.nrl.eecs.qmul.ac.uk",
   "planetlab2.cnis.nyit.edu",
   "planetlab1.tsuniv.edu",
   "plnode-03.gpolab.bbn.com",
   "planetlab-2.di.fc.ul.pt",
   "pllx1.parc.xerox.com",
   "plab3.eece.ksu.edu",
   "planetlab-1.cmcl.cs.cmu.edu",
   "planetlab1.cs.pitt.edu",
   "planetlab-02.vt.nodes.planet-lab.org",
   "planet2.elte.hu",
   "planetlab1-santiago.lan.redclara.net",
   "planetlab4.cnds.jhu.edu",
   "planetlab1.thlab.net",
   "ait21.us.es",
   "planetlab1.montefiore.ulg.ac.be",
   "planetlab-um10.di.uminho.pt",
   "evghu8.colbud.hu",
   "planetlab1.s3.kth.se",
   "planetlab1.jhu.edu",
   "planetlab3.informatik.uni-erlangen.de",
   "zoi.di.uoa.gr",
   "kostis.di.uoa.gr",
   "uoepl2.essex.ac.uk",
   "pl1.uni-rostock.de",
   "merkur.planetlab.haw-hamburg.de",
   "planetlab2.tamu.edu",
   "pl1.csl.utoronto.ca",
   "node2.planetlab.albany.edu",
   "planetlab04.mpi-sws.mpg.de",
   "saturn.planetlab.carleton.ca",
   "planetlab2.cs.unc.edu",
   "pl1.cs.yale.edu",
   "planetlab-1.ida.liu.se",
   "planetlab-2.ida.liu.se",
   "planetlab2.mini.pw.edu.pl",
   "planet-lab2.uba.ar",
   "planetlab5.cs.cornell.edu",
   "planetlab3.inf.ethz.ch",
   "planetlab3.cnds.jhu.edu",
   "planetlab1.just.edu.jo",
   "planetlab2.cs.vu.nl",
   "planet11.csc.ncsu.edu",
   "planetlab1.cs.vu.nl",
   "dfn-ple1.x-win.dfn.de",
   "onelab2.warsaw.rd.tp.pl",
   "planetlab2-saopaulo.lan.redclara.net",
   "planetlab2.sics.se",
   "planetlab2.cs.uoi.gr",
   "vicky.planetlab.ntua.gr",
   "planet02.hhi.fraunhofer.de",
   "planetlab1.cs.purdue.edu",
   "planetlab1.otemachi.wide.ad.jp",
   "planetlab1.unineuchatel.ch",
   "planetlab2.montefiore.ulg.ac.be",
   "planetlab1.fri.uni-lj.si",
   "planet2.inf.tu-dresden.de",
   "evghu1.colbud.hu",
   "planetlab-2.elisa.cpsc.ucalgary.ca",
   "plab4.eece.ksu.edu",
   "planetlab2.cqupt.edu.cn",
   "planetlab2.cs.colorado.edu",
   "planetlab-node-01.ucd.ie",
   "planetlab2.ics.forth.gr",
   "planetlab2.exp-math.uni-essen.de",
   "planetlab5.williams.edu",
   "planetlab2.csee.usf.edu",
   "planetlab-1.di.fc.ul.pt",
   "planetlab2.pjwstk.edu.pl",
   "planetlab1.wiwi.hu-berlin.de",
   "planet1.colbud.hu",
   "planetlab1.informatik.uni-wuerzburg.de",
   "rochefort.infonet.fundp.ac.be",
   "planetlab-4.iscte.pt",
   "onelab1.info.ucl.ac.be",
   "planetlab1.polito.it",
   "gschembra4.diit.unict.it",
   "planetlab1.mta.ac.il",
   "planetlab-2.tagus.ist.utl.pt",
   "ple1.ait.ac.th",
   "planetlab3.bupt.edu.cn",
   "medea.inf.uth.gr",
   "onelab2.info.ucl.ac.be",
   "planetlab2.cs.ucla.edu",
   "planetlab1.ecs.vuw.ac.nz",
   "planetlab02.just.edu.jo",
   "planetlab2-santiago.lan.redclara.net",
   "righthand.eecs.harvard.edu",
   "planetlab01.alucloud.com",
   "planetlab1.williams.edu",
   "planetlab01.ethz.ch",
   "evghu2.colbud.hu",
   "evghu10.colbud.hu",
   "planetlab1.cs.uoi.gr",
   "pl1.sos.info.hiroshima-cu.ac.jp",
   "dplanet2.uoc.edu",
   "planetlab4.csee.usf.edu",
   "planetlab4.eecs.umich.edu",
   "planetlab2.cs.ucl.ac.uk",
   "planetlab1.extern.kuleuven.be",
   "planetlab2.extern.kuleuven.be",
   "nodeb.howard.edu",
   "planetlab3.mini.pw.edu.pl",
   "planetlab-1.cse.ohio-state.edu",
   "planetlab5.cs.uiuc.edu",
   "planetlab7.cs.duke.edu",
   "planetlab2.sfc.wide.ad.jp",
   "planetlab2.cs.otago.ac.nz",
   "planet-lab1.itba.edu.ar",
   "pl1.ccsrfi.net",
   "planetlab3.rutgers.edu",
   "pl2.ccsrfi.net",
   "planetlab2.clemson.edu",
   "planetlab2.cs.okstate.edu",
   "kupl1.ittc.ku.edu",
   "planetlab2.pop-pa.rnp.br",
   "planet1.dsp.ac.cn",
   "planetlab1.cis.upenn.edu",
   "planetlab3.ucsd.edu",
   "planetlab-1.ssvl.kth.se",
   "pl2.eng.monash.edu.au",
   "pl2.zju.edu.cn",
   "planet0.jaist.ac.jp",
   "planetlab4.cs.uiuc.edu",
   "osiris.planetlab.cs.umd.edu",
   "planetlab1.cqupt.edu.cn",
   "planetlab1.umassd.edu",
   "planetlab1.cnis.nyit.edu",
   "pl2.sos.info.hiroshima-cu.ac.jp",
   "planetlab2.cs.uoregon.edu",
   "planetlab1.uta.edu",
   "planetlab-5.eecs.cwru.edu",
   "miranda.planetlab.cs.umd.edu",
   "planetlab2.georgetown.edu",
   "planetlab2.iitkgp.ac.in",
   "planetlab-1.elisa.cpsc.ucalgary.ca",
   "planetlab1.sfc.wide.ad.jp",
   "planetlab2.rutgers.edu",
   "planetlab1.utt.fr",
   "utet.ii.uam.es",
   "planetlab2.upc.es",
   "dplanet1.uoc.edu",
   "planetlab-2.imag.fr",
   "lim-planetlab-2.univ-reunion.fr",
   "planetlab-1.imperial.ac.uk",
   "planetlab02.sys.virginia.edu",
   "planetlab2.cs.stevens-tech.edu",
   "pl2.ucs.indiana.edu",
   "planetlab5.goto.info.waseda.ac.jp",
   "planetlab2.dtc.umn.edu",
   "planetlab-04.vt.nodes.planet-lab.org",
   "planet2.dsp.ac.cn",
   "planetlabone.ccs.neu.edu",
   "planetlab1.eecs.wsu.edu",
   "planetlab-6.ece.iastate.edu",
   "planetlab02.alucloud.com",
   "planetlab2.cs.uml.edu",
   "onelab1.warsaw.rd.tp.pl",
   "pub1-s.ane.cmc.osaka-u.ac.jp",
   "planetlab2.cs.du.edu",
   "planet12.csc.ncsu.edu",
   "nodea.howard.edu",
   "planetlab1.dtc.umn.edu",
   "planetlab2.cs.ubc.ca",
   "planetlab3.wail.wisc.edu",
   "server2.planetlab.iit-tech.net",
   "pl1.planetlab.uvic.ca",
   "roam1.cs.ou.edu",
   "planetslug4.cse.ucsc.edu",
   "pub2-s.ane.cmc.osaka-u.ac.jp",
   "planetlab2.poly.edu",
   "planetlab2.utdallas.edu",
   "jupiter.planetlab.carleton.ca",
   "ricepl-5.cs.rice.edu",
   "pl1.cis.uab.edu",
   "planetlab2.ustc.edu.cn",
   "planetlab-2.cse.ohio-state.edu",
   "planetlab02.mpi-sws.mpg.de",
   "planetlab2.temple.edu",
   "planetlab1.jcp-consult.net",
   "planetlab01.dis.unina.it",
   "planetlab2.hust.edu.cn",
   "planetslug7.cse.ucsc.edu",
   "pllx2.parc.xerox.com",
   "pl1.ucs.indiana.edu",
   "planetlab01.cs.washington.edu",
   "lefthand.eecs.harvard.edu",
   "planetlab02.cs.washington.edu",
   "pl2.6test.edu.cn",
   "plgmu4.ite.gmu.edu",
   "planetlab2.jcp-consult.net",
   "planetlab-2.cs.colostate.edu",
   "planetlab1.utdallas.edu",
   "plab2.nec-labs.com",
   "planetlab1.di.fct.unl.pt",
   "salt.planetlab.cs.umd.edu",
   "planet1.zib.de",
   "planetlab1.hiit.fi",
   "planetlab3.cslab.ece.ntua.gr",
   "aladdin.planetlab.extranet.uni-passau.de",
   "planetlab-2.sjtu.edu.cn",
   "orbpl1.rutgers.edu",
   "planetlab3.csee.usf.edu",
   "planetlab2.cs.purdue.edu",
   "planetlab-02.ece.uprm.edu",
   "orbpl2.rutgers.edu",
   "planetlab-4.dis.uniroma1.it",
   "pl-node-0.csl.sri.com",
   "roti.mimuw.edu.pl",
   "plab1.engr.sjsu.edu",
   "pl2.csl.utoronto.ca",
   "planetlab6.cs.uiuc.edu",
   "pl2.cs.yale.edu",
   "planetlab1.csee.usf.edu",
   "plab2-c703.uibk.ac.at",
   "marie.iet.unipi.it",
   "planetlab2.fri.uni-lj.si",
   "planet3.cs.ucsb.edu",
   "pl2.rcc.uottawa.ca",
   "pl2.uni-rostock.de",
   "planetlab-node-02.ucd.ie",
   "planetlab1.mini.pw.edu.pl",
   "mtuplanetlab1.cs.mtu.edu",
   "planetlab-2.cs.auckland.ac.nz",
   "planetlab4.goto.info.waseda.ac.jp",
   "planetlab-01.vt.nodes.planet-lab.org",
   "plink.cs.uwaterloo.ca",
   "pl1.6test.edu.cn",
   "planetlab1.ucsd.edu",
   "planetlab-2.webedu.ccu.edu.tw",
   "planetlab2.c3sl.ufpr.br",
   "planetlab3.canterbury.ac.nz",
   "pl2snu.koren.kr",
   "server4.planetlab.iit-tech.net",
   "planetlab1.cs.du.edu",
   "ricepl-1.cs.rice.edu",
   "planetlab2.eecs.wsu.edu",
   "pl2.cis.uab.edu",
   "planetlab3.postel.org",
   "pl2.planetlab.ics.tut.ac.jp",
   "planetlab1.fit.vutbr.cz",
   "planetlab2.tsuniv.edu",
   "planetlab1.cs.colorado.edu",
   "vcoblitz-cmi.cs.princeton.edu",
   "localhost"
]


# hard-coded whitelist of users
users = [
   "jcnelson@cs.princeton.edu",
   "muneeb@cs.princeton.edu",
   "wathsala@princeton.edu",
   "jlwhelch@princeton.edu",
   "iychoi@email.arizona.edu"
]


# test keys.  DO NOT USE IN PRODUCTION
volume_keys = [
   { 
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxwhi2mh+f/Uxcx6RuO42
EuVpxDHuciTMguJygvAHEuGTM/0hEW04Im1LfXldfpKv772XrCq+M6oKfUiee3tl
sVhTf+8SZfbTdR7Zz132kdP1grNafGrp57mkOwxjFRE3FA23T1bHXpIaEcdhBo0R
rXyEnxpJmnLyNYHaLN8rTOig5WFbnmhIZD+xCNtG7hFy39hKt+vNTWK98kMCOMsY
QPywYw8nJaax/kY5SEiUup32BeZWV9HRljjJYlB5kMdzeAXcjQKvn5y47qmluVmx
L1LRX5T2v11KLSpArSDO4At5qPPnrXhbsH3C2Z5L4jqStdLYB5ZYZdaAsaRKcc8V
WpsmzZaFExJ9Nj05sDS1YMFMvoINqaPEftS6Be+wgF8/klZoHFkuslUNLK9k2f65
A7d9Fn/B42n+dCDYx0SR6obABd89cR8/AASkZl3QKeCzW/wl9zrt5dL1iydOq2kw
JtgiKSCt6m7Hwx2kwHBGI8zUfNMBlfIlFu5CP+4xLTOlRdnXqYPylT56JQcjA2CB
hGBRJQFWVutrVtTXlbvT2OmUkRQT9+P5wr0c7fl+iOVXh2TwfaFeug9Fm8QWoGyP
GuKX1KO5JLQjcNTnZ3h3y9LIWHsCTCf2ltycUBguq8Mwzb5df2EkOVgFeLTfWyR2
lPCia/UWfs9eeGgdGe+Wr4sCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA3wIape2VASiHYTgirZRz
nzWd/QSK8HuRw3kqEKUGjLiRCb072usuwQ+ozez+6yj0gA/3otKEjm1KM2K+qnk2
JZW12YEodF02KoHVtwf3x7GPO5drnO4TPlEFCXBJjPqjI6/YVhfZu3QNdPnulJOW
yKKP+0ij0sxJ4vuglq2FbuEfneptMEWdFQjFAa10Tc1F5LBNAUK+lxVszEBnpwRQ
lcM11ro8RSuZrlulGK6tEaJsncUBvhqESRMTJ9sbngksxlmYbfhBTkRt2Lnu+F4X
gp9nQ9qp3IU+/065Z4pACnDpmbZReypnHWsirptlxAqW5Va87QfGNMgtduBkuR0f
5wYlAI61dJzg6clXjXlyDbATXdweilr52J3Q9F2MHVTpUDBpXhWoko1kaqoMPgLq
RvAenNeQWNEzlHgqOFAAL7ibq0bbkLlYQwP+v9udEaRkRJyxPbbafRzNJPPE8lUs
pbK7f3xY7NHfp4pK+RsZ092S+gSUp8kN+SyOSnq7j0vnNQsW17DXqbH1Vy9rWhAJ
3km0KP/QAjOTROCadZdY1JoZ9dQU1MOBxTSTF72jsSe45kJwfNMkmIeYf+bdbvq1
uW1shmufG1hu3OcP891hvXDE0Qg3+Uev5JgdOSd+akPcXUPFE+OEJ8NTnACc4nfK
e/VhFoqm9R1zxkmHfaxzLKsCAwEAAQ==
-----END PUBLIC KEY-----
""",

      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
AatQy5UCggEAB6h4RJJPBx/dQoOof8ExReSn2DlcOvyx0GyNH3bh2UnmVmRk04V1
dXlcIiTK3VKSVSTH9UOzOALR8LouLzsa98nvXseRw59bICLeA3ub793Okq5iH2Wr
06WRaDRqZPG98L/C5dQqaaBNxO4rFfUOmQlCmb8MUVGQN7GHPmBADuEJd9RvRFzS
2up9hBI3AFUqmfIjb0ccXocyIx5FHOyRwqR/aormQgANvQm7PuCwUwRsNQysq1gS
tHuEnlJ+QhyUIWRXqFmATXznWcEZT2612yCbAtA3xYeBPo78hoVy1JqZbh0gmIHR
Xqd8gaFPA0+MFlFowXn1BazHES3HWq2jCQKCAQEArJc/J/F1KxpdTa4gdkEMGFtb
mnZFKfRm/HEggWK0M1oBn1i+ILfrFSstKwte41jS61n3kxBMewCNBXmxzU/zdz2e
EoVDB8tNpaSP/48TZCNNnp6sS82NdhVJC4d2rCDaKHT4vW17DIKEImhDRZ5SJ50J
iGs/7A4Ti6bnl2qrvyYf1vKG/l6mze3UIyx1WAxKGRJ/lgVCquSGHiwa5Kmq4KTN
5YT22tp/ICBOVWbeMLc8hceKJQHnP5m1SwjgKdFwFM46TWfJIZWs7bTFbw6E0sPL
zTnZ0cqW+ZP7aVhUx6fjxAriawcLvV4utLZmMDLDxjS12T98PbxfIsKa8UJ82w==
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAwSxr8PvzgicDAe0T7YK5
/ARQaIrmNkNRE1u6nxaQ8Z8XYRLeP2TcyJ+ce38vdm7t49aNZwz4hpEiASwIrLV+
mlAhV4capmGWS6rhziI1i0Q8lMxqymtIgRZ2myMdVGKRV/iCW9Z82kYsflOf6eIf
8IZ7qTaIapK5w5QV5SVh0MU1Dq1pad9cQL8D4wf56co7W9Pj9nGi/jBv2otVVfaF
AH5rv5oHubkPbeKk+hMRvJ9PCOUrsqyGFr1S3cCvwKzpxtrVci/t6rG7oOUS+CGy
Zrr/J8S4EzsoFzBKbCH4hYkfb5tCZwAq1IDbDz+hYvs7cJaDDEeQFagwjOhksmpB
pGoJhvu7KgY9hL92xuyVYx6fjDC5Y7pEYwnkeT95DPrwltONLCLG71GCJcqpzC7b
lAuB/yRaNOroIGRfNI0QFzwF3xdQ+pRd+uvUdh4PuFsvNGWWT/qOq3l6GZ0R0aI4
4E+fEQQrOWCa+7gmMtTaSnSOyeQUbZnUvDfMc4sJ4KruWJCU+2CMtlyCLJKwgIIB
Tzg+TvDhdbCEH4nZuAi1UgyZDg+gpFMrS06eD0AeyRWNAoyhMZF+Sycbb2Ym9LWW
kTZ6RkQoWyZ4ziCciLgUG9q8WRMnV7zn6mia8FZS4Ry/wvQxsYR4kL5DYOEDFKq4
f6fgXIDYOmZtPBoJ9A3yOq0CAwEAAQ==
-----END PUBLIC KEY-----
""",
   
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAodsgs56qQZYPErBwyfSr
4ojWnWKCe6Ywg0iNBqGGuDN10pzyOeTMRLwyr77x5DJz6Dkedo6uMBdU+19QKtap
JZ/ojt+x4A7T8QYfq/3whX42yzj5jV+6D7gci60eCuVaWjFYJXQvDnYY6vhyxab0
qqKpaoM1XehlgT+n1rQHdXAFtXwY8l3ND59JvW3wKQd9bc2UH++HgSiaXfryFeGM
pY+dslq6p21gvNIVcoIXKOhvf9Ta9mDQ/lksdB9Q/pl1HD2N6jTR88buJf1OET1g
1ASyGsrnHxhvUBKzEgQfaQzARYtWj+e9W6kFCQ3C63T+3fV2odJLDxqpusuuJfcT
RHZ8ZyMCJhJt7pbNrgVCd1TOgAE6EJQMpdkVLzQoZV+6lx9jI5BvmIXuBLXG3mTm
15CJ2Kx/RH+72/iF28BYiQf5ZW8/b949wZKg5vOarDMw/ZaGWey8P424iXuQqv7P
AvhMjmvtFkW47Zhp/RmqPIq99w+zsLMtTkrFbKtymF8Ml1st5f4q04N/VYtg9Jvd
quN6+MK26q6SKXryCVIgk3C3xIswazFJ+g7zGjLozAYt7UvBk6BlvxyHGpsnp8H2
rTEX7y0ZgQvgHqJxB17XTUxMbyQ+rqvwk8Rx+HAw0cwSRifZZC/fBuhi7jWXLGnC
8WVmUdpR6PGRbkqQ1edEI1sCAwEAAQ==
-----END PUBLIC KEY-----
""",

      "private": """
-----BEGIN RSA PRIVATE KEY-----
MIIJKAIBAAKCAgEAodsgs56qQZYPErBwyfSr4ojWnWKCe6Ywg0iNBqGGuDN10pzy
OeTMRLwyr77x5DJz6Dkedo6uMBdU+19QKtapJZ/ojt+x4A7T8QYfq/3whX42yzj5
jV+6D7gci60eCuVaWjFYJXQvDnYY6vhyxab0qqKpaoM1XehlgT+n1rQHdXAFtXwY
8l3ND59JvW3wKQd9bc2UH++HgSiaXfryFeGMpY+dslq6p21gvNIVcoIXKOhvf9Ta
9mDQ/lksdB9Q/pl1HD2N6jTR88buJf1OET1g1ASyGsrnHxhvUBKzEgQfaQzARYtW
j+e9W6kFCQ3C63T+3fV2odJLDxqpusuuJfcTRHZ8ZyMCJhJt7pbNrgVCd1TOgAE6
EJQMpdkVLzQoZV+6lx9jI5BvmIXuBLXG3mTm15CJ2Kx/RH+72/iF28BYiQf5ZW8/
b949wZKg5vOarDMw/ZaGWey8P424iXuQqv7PAvhMjmvtFkW47Zhp/RmqPIq99w+z
sLMtTkrFbKtymF8Ml1st5f4q04N/VYtg9JvdquN6+MK26q6SKXryCVIgk3C3xIsw
azFJ+g7zGjLozAYt7UvBk6BlvxyHGpsnp8H2rTEX7y0ZgQvgHqJxB17XTUxMbyQ+
rqvwk8Rx+HAw0cwSRifZZC/fBuhi7jWXLGnC8WVmUdpR6PGRbkqQ1edEI1sCAwEA
AQKCAgEAiuj72eyUhpF5Ajs3sbwxQMzcFFsVTXXGMQY1MrmyW6ieuFFGenVo8pzq
i3a/N3AtleJfyLSPvmpn3w1gSkPNC6N//g7yJC/VqgftarkYkhlOgX+2faTolNEY
fq/nFsbckofC9PIP6MNmg1MTfZraZRARgn51cNEhPJobO+Nqe1nXLHkDGA91DLFS
oicWdMAe3/wN4pK6oxjr0ziqSCk5gmYNm6LOix8OZT+QdKQ9qDhEaVuPSOCyvXhe
9JJj+v5NSh1yDM2kWyoh/ay2MYmR7qTRDrmRPljEP96snQu4wXnWElmRwxPKqj4X
Ojv7TAUvL+OZGyzWinIBtXVVUvQILdxULvWxJToWImRq1O6ReOKR+F5xZLwTZkiI
sl5XiB2hzsyz9XVFaI8XpDsCpap6UQYEsiIPRRl7mKU+l/OMJQGunwBsAVfhqURg
YFHybWXLzuPwudGq6CYT6k+oYk669XjNn+GB+F+WaHw0WxNFKKvVJl77kQglbkYZ
Zi/lbsCngmvXWcYF8ni+00EDFKraIVEspYpsd0tx7ZTsrtSDlMsrtoSbvkX2lvsM
1XOivpjWEt8WjCFzXuBCpgXFh7oImGUYyYxRgrERRC2Ay2kMB8bXX6QJHz+RG5M+
8CiIcnon/yy0wC1rmETy3rTJyAgxRgz+noLa/cf9HbtfAasi1dECggEBALZRcBBI
zEkKdQglWFa3wBahXoLIhZCiPE/UTCXBxjlL7tizRkVBufh80dy5l1lpAYMb0V/5
+ntGjD9XxrRv2oX/r5Djqgq2UvXhUi6RvYENxrds3yZyquMv7t9uqBz62uIpcg88
wFDmL7EXn1wzYjJkBObU8pVp6a75M+fRIEkIWYg2CrD6QqkOFDi8Br6OIQ3kpdYG
GkZScoTY0TFsShyS7gltz1/jqzjUYgWDy0Swf6OrACpY8oxm8Ek3Exz009K0uYVb
e1UqwPPfSBFBJ7sKdzDmGkCO4gKbYVewma3DJy1wqhIG3Z0RBpa4em7BgoY2q5zk
yMCMAePlRPMjvj8CggEBAONErxjp2xGyERvBbNectvOOYbHP5ePDrrWLJzRn4oiL
F70J/b+4IjPnyzxshIxAcIbRXhBjy12iyezxQFt6BlCVv6rp4PQO4Kci6JG39yIY
7MshLDAM5IoSjmCpIBQ+Z4QvlB+U+Rvs8toqDYb925sc1A5MkZYZ4KDS8oxAKA2d
CZFGGPnvpqkd/cDCmMxpghTz00nSdabqpMnB2dyx4JjEo+S5s7kzReF9hgcIvosf
43qV6ReChmZWJzR5zwXziZVdDZj7UQKQF9dYg2zUgbrhP5Vv9Y2UWL0VjrbhAWOi
ZaijaxWuHdcgL7wtzfZ8ENsthYR8TB4S85qP8YHRy+UCggEAMInt8+ftu2R1U/3d
TvkMwvmjV95a8O7Ab+BthX/Zblew9zCDfNzKzkUs+j7O9JboOCenzo2XCkr1+8c6
t78vxo0UCNcT5lY0MBIU9yEF+t3YEe2CW5IbvPB+AC5Nw6llrOCr5TKfYOpnuBY5
7yoe8pt+UpZPH0hbVqIyF32twsPeUk1Q+10lciy1ZYVppkHguvoszJZDWIWKx6OR
zj6uXH0hspDxrO3IIBErJ5y2+UqkMMbN/HhB2u9s+ZEYHZVw75/95Gs+oqYHSOYh
IbbfBQT8RqB8TMO9mEQV3mq9/2z9eTONk88QIUjvICFtNx99yI21kse2Ssz7k9ju
Sm0xsQKCAQABhpyGVkuaOs/Sl+HbMLopuHL/dp3cgZvSrDR/LZNfx09JfZ+ugdX4
8z1yEmhxaJ1Yyl8PTRw4bLdeV+BOvOr974Nx6lAQU2k+tgVJtSp7mB8+3eImEHAY
XTeHfpswh5q6UHd2HtHtVhHrVQzyMxIeYHsr/Wec0l+ntMLcHjImpT9DMm1IgEtm
J0Vk3emYtbyh12E7xaX0kO66TAriaG9Rl0BicLy8KZP2h5k2wuBEntowIIGIHuA4
H6vztj2llJT/47gLpuRMWBtQ3s9DE8orLwf2QOItKSnPWy2yHKCdwqFcGHzHkHuy
zgyD/uq7FOooDo4u7Ya1TPtFtmGwtW/hAoIBAHSPgY1a1+KA4fCeGRzHN3rxhgN3
ZjGfhNhKbh0IkEWjZhdOLaFC7cudV//hjEZ0ek1mUYYoSuTdQM6ak4xxYzg4kq4q
/HaHlTPzawwzwVhHCbmLitrSZdjibGx4MkNzCSzbh9EAToseng/gjLEYO00RIZPM
41mRvwTJQYH90WxnOFBh7iwKJsn5ez6TNR6Bj963LTIlPJq3n5+h8AeRJRRHvgGY
Gek8NSp0YI/ANOOK7POSPzGVtLyJzPCWbfZ95jl1dh2lmAlhr4SngQeYNL51vQ6k
UxFm2LniI9MlEj0FmaQS8q9F5c+0cgR8+1z0CYIcPm0v70fgRP6UaRLEjxM=
-----END RSA PRIVATE KEY-----
"""

   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEArZak/6bUWG+0SLf2Nzs+
xo8WAYQrYmINcyknKWXd5UK4gAkLxzPOZ3O4768MwBraCdU/hBU7QAZbYhjoSUO8
OwluqpbZU1wsSxNfliANomjYbK7a5irjgY7sW7P8KxZZQw12/iKIlLYRD5g5xqrk
1V0FKexhGqWLQmyxbmBvxgPQVGaRwnX6Y4JTuY2QNhgurPrsmMdnB3RTmezJSYVE
Q6bv9b9ntQOLK1rGl7YJQ4pNaIr5nrBnGWP579lQAAKKlMz6EMw4PIYbi5ZasyJh
cmByP5qE/+NUuvt8Q4wF7j0nrby58J9/pMxehPtGJI8U42ezpoHB5o7jypwAo5bX
UWC0o5M53nuYRuWFi8IqYpNVbj3m3JgNTRomwtFPqGd1HGWT1i0mUToDAmcWagj/
68nXHi6Gfaah5xy15y+gNSyhfVlcFc8nh/LCQWpUzLr8bGU6YzX3JIQAxbOjBFs9
O+nIXpoYY/D6fN9n5GKqzX1MNxNY+0DR70cwLyV+sNetqeWfo8Km01oLgnN/Oaxa
v3zTDm7n8ypEetdkIlbZTIywVrJllwOzUYtUif9Lv/+O036QHKao8WbT9/uEC3yf
D9qQGBCz/GkoUNb7vrkpm+3euVD27mPZ7xuyDXdHZpz0FBq9lhvSmtCIdxDoFbgb
EcPV4bPtr4Vuo2ruJy4gFLsCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   }
]

# test keys.  DO NOT USE IN PRODUCTION
volume_archive_keys = [
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA2ZNxxawwAR7h08Y1rIgl
Wbw33VEDSvG+BYxADXNEcJmc/D7m6qMBXzc8oT+P7fK7OrjtY+zVe4K5JKBySi5z
sXEqCifaphlhNjXuO+hSN2VW0UOBxid2X17dULgPh+jgxYXBpgv3ffeq9fomBM3E
MovYc3hsQpGp90SGUTPuGA6ADw7dO5lEqB+9N9v9xKSiLI6zp1DVVnfUPhajNM38
uQr/3GMMaUyZhEdzJjdFWr3gM1bh1PZa5aogM1E71J0M7OMtXHW37JToMoSE2FW4
cktWEQMk4aogR8xOMLa8Wri8Jiwef6i+LT8yKDqEH3/DGlOaxcngmCZKYmsirf2E
oRwzapA58DivxYGOqfEFjL3TwJSEEUS01lM6a2U4/joaImifDLtW6/i/eQPqANmF
TA1jpMVEIAir3EN+Xqis3Sfwkctnsn7MEFqO4w2NW0CCmPZih2PZc/L+lhWmlRre
r/M+eFUwREFgBzw4zPKvcK+jGDXy86Kb4KepvOCKkWkpNTg1EfqlYmfzH7SLrSoD
sGzOaeW2dqJGAMEHUXfL0r68UqBF6GnPLxziSOQtwPIa3cd355LpBdQoMyHm5UuJ
0sJ9K3IV1lKKss3Q3Z5GtQNOK+lg8qCCMkz+FxS/FZLJWcROjfh8v9QCjr7YaswD
yFe0p4vgSilzjIt8f/0oVjsCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAuncjSamzRCq5cjGCAwgn
ybAAHMAi/wen3SIWjQfIpQhzM6HYR17r0cysa8tZGCI045aNbEeVbVEfaTPOf9SG
HOmtycrC8EKCE+Z1bcPZfStSEt9D/mmimjlIUJYjBJ/2H0unDDNRhyWBa2YznMZH
fECZ7bw2OuChzzrzoYnd99l2edjyNTdRD0R+rRfsm36AC9Hm6kRMr1FqnylMNzMv
WyuiWELIPEOeUz91MC0G4TWEds9dYI/ptQDRKmvDu/5oZTUIJX/nOqxW1j4kzGpU
1oKPtGIMSPSsxT95RXU5Z7dLK4ox0hwAHee/Gs0NZcS/iBz9u1j9OgWBjxDQwj73
s9khK0Myh8YS/X4b2d5pXh2bdeky6pvfuEniroIUQF+ppAAC2lcyCME8sqC59UQg
P+MdiG5wP2/7fSUz+rn3Y29GKgjQf+54jJg623NBacooedW+0u5AT9H/mhty+pnT
wbXCssMc7eeoTAkoAIBtOZqZCaq1NRPDbtQGfkOzlBPOqzOHkK1aSSL/jT9rj3Nj
OMVVJgoR1LGhsPBQd6hQZtuzdlXCNTbzLvyCFtG6bD6iApAue7fXTiSP90XRYm5R
WAD2s5BJOzDkzLWxsvS0hqu5cOQlouqVMg3kvBew7NkJz79+THbOu6uKEAFPNDju
teU1YhCDaIB9LAq54EUzoocCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAh/t7kqWQ6+48ecyYxCfL
xiVFYXLft+yTj5xVHaAZ+oBRMeex6OY6A74hnBL8o2ghva/99WEDxxYsnD8YDacM
vHiHChunc/MHiK68Tc/6vS1PHD5hY4JZA3J17hUIeK27Q/EmaPqoafpkjhi+Xz5S
MO00Ms7ieu3FfeOlyELHK9m60Jby5jhbBKmx2koz2bpkZ7OnXMcICSyWEp9AvN8x
8iupkGhUER4Yx2pyhUZQ0fomcQGLtTghq08sr9ZmlZmxhRZBxQgFYSW0GhIQYn4t
j8WSaYamA80EclqKsv0+K7BqoIqINoMIVklsPJq95c8lGC0WY0OAcjpoCHQdx7ko
tkrG8s8p8Pd8Pdk2Nt27EU5NdnXMKJD7chZ3EJYJPzXS8lInn0xq0W3/5N952p4k
fuCbgxgESpfKY+uhhfqkIOTw9qcB28RnAH0wsaSNLXRCmaB5nh9I9ngLp4+6CCBh
ngg1HAplmwvtpyPP6DvThCzmxDGLUrYP7iQ1Y0THPnbDsDB5Mwk+2v6f+zO5rsws
amvZjNtwPec3olCcpH3CsOIRrWljqfVCjk/MnNQZHL9Dthl5d4T3hr9UOgWqiAoz
xwY1nPp7Zp0zitExDPWqqv0mucuZmKCNNBkUiDYScafqt22f3vCBSYi4sqfOfwTG
Kp5w7zzdz6jvDkzLM8osPQsCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA3/bnhsyMvMYjAAdB05Jo
uAUXFnh/ucdw+uc4CmNdzBESwvuN2XyR2R6yZ0DLLYJfm86KkYlq2z044hD+rbhE
FtjHglwUcNWVHMhdNyPwybsheZygtKzqZ1lqHWOH44WN4NKhB6h/UqKeNWBNu0P4
MSPzKLyyEDhC5GB+qflQZ6cUMVv3fRrNbd3LVzbpxLjcjJz813ETTqOz1quognfI
LBGRe44+Y7s7NBVU8q9l/lEIPajh2oA4X/Jg0ljbHfKGo+8ngezw2nQQ2PXANv6b
9emXF1jU6goor0QH/I/MnTsbJayZYxv5rP3ThxEy3lnSacpoXr4wO7tFJAGeqfNa
hxNlPOxG89hNxs4m+h3wrnNaltCK2yl9zq8lSFJvDcmi9w3/h+x9nOGOZmDqSG8c
4m4LZdT/7JjVXfQezjiKHNPACs6MgQC3jsR7gGLnJ6LxbJziZiHDwSP71+EPSTjU
PCTeQlZz9MLnr7e9IPYz4OdU+wpQu6Wqaof219SSSg6XuVs1WWvgpIRvbAEK/j0+
L2qYnZno0sM7Af7ci9AO6Cbcj97Ts16SzudDnrBIpM4wDbxcX6s/PVgjS6Rzq+kZ
qRK6zQwN/Rw5KshnASwp5tgpqrzd2HwXfS0BwkTNrZe7aXqm1eUp102Ksva6q3Oc
9+4AAmKwOlIuZCHiOxlwulsCAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   },
   {
      "public": """
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA2s0mGAgF9NoQxbRZSydh
JvETVNCxUXK2dvnSe/u8HWltLt6y3L0rxSXuad2XSVRHiJ0XE4kAa/CFKY9oPvPe
2ZlPxpLkiLZsqtouwWvnfQOhAmhdTihXnClHQYKotQ4lYR/oZFjEWvUxrcYbN7UT
Bnb4ne/04M2wLfOJhsX8BacE38BaGgiGlqRPlDa40Z9k+mUZ0gElcmsqpfRC4/0l
LFKd8uk/FAj1KX3z9D4DV6bcrNdDATp/flqKlF3CQeRatFpEGOVIYY+j0mG32lkC
3B81MyJMzRwOSXjLnWI1IgUYBT+R6MQZ84aimlsBRfU6iKabMAqfxPj/mxQBUp5u
fqipu6J5Fdn3hjMXuHmS6Llqz70OMqT2AlYGBuGbohDE4uLci3qnybtUbs6jREfa
rKFqPkkLlI/mrz6iTagsGeAP9djaZTTVgc4TU/gax0HfpJKBoCNY5q2rwdzD/yks
N8rcqLpBnmomAx0p2O7lR49dfnGBWu3cBbexeclAd4jTpPXTSVyMTlMddTRTgEYe
32nHb+PEUe/DRuZZb7nzxtuPBGmuJWYfz2vgLqVoS+ZrBupNUgJBy5P1n4JfU0oA
PhhxCWtck6oqW1YqMKJ4A9CMoHotAYvgPZrWyDaamxND+aXPp2y94tNovDrr3DWJ
gOPgpaSQvqFtPgI05I6xzt8CAwEAAQ==
-----END PUBLIC KEY-----
""",
      "private": """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""
   }
]
   

DEFAULT_USERNAME=users[0]

def testvolume_name( username ):
   return "testvolume-%s" % username.replace("@", "-")

def G_name( gatetype, nodename ):
   return "%s-%s" % (gatetype, nodename)

def get_user( username ):
   if username not in users:
      return None

   try:
      user = storage.read_user( username )
      return user
   except:
      return None




closure_str = """
#!/usr/bin/env python 

CONFIG = {'foo': 'bar', 'STORAGE_DIR': '/tmp/'}

def replica_read( drivers, request_info, filename, outfile ):
   print ""
   print "replica_read called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "outfile = " + str(outfile)
   print ""
   
   rc = drivers['sd_test'].read_file( filename, outfile, extra_param="Foo", **CONFIG )
   
   return rc
   
def replica_write( drivers, request_info, filename, infile ):
   print ""
   print "replica_write called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print "infile = " + str(infile)
   print ""
   
   rc = drivers['sd_test'].write_file( filename, infile, extra_param="Foo", **CONFIG )
   
   return rc

def replica_delete( drivers, request_info, filename ):
   print ""
   print "replica_delete called!"
   
   global CONFIG 
   
   print "CONFIG = " + str(CONFIG)
   
   print "drivers = " + str(drivers)
   print "request_info = " + str(request_info)
   print "filename = " + str(filename)
   print ""
   
   rc = drivers['sd_test'].delete_file( filename, extra_param="Foo", **CONFIG )
   
   return rc
"""

driver_str = """
#!/usr/bin/env python 

def read_file( filename, outfile, **kw ):
   import traceback

   print ""
   print "  read_file called!"
   print "  filename = " + str(filename)
   print "  outfile = " + str(outfile)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "r" )
      outfile.write( fd.read() )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def write_file( filename, infile, **kw ):
   import traceback

   print ""
   print "  write_file called!"
   print "  filename = " + str(filename)
   print "  infile = " + str(infile)
   print "  kw = " + str(kw)
   
   buf = infile.read()
   
   print "  Got data: '" + str(buf) + "'"
   
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      fd = open( STORAGE_DIR + filename, "w" )
      fd.write( buf )
      fd.close()
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200

def delete_file( filename, **kw ):
   import traceback
   import os

   print ""
   print "  delete_file called!"
   print "  filename = " + str(filename)
   print "  kw = " + str(kw)
   print ""
   
   STORAGE_DIR = kw['STORAGE_DIR']
   
   try:
      os.unlink( STORAGE_DIR + filename )
   except Exception, e:
      print "Got exception: " + str(e)
      traceback.print_exc()
      return 500
   
   return 200
"""

json_str = '{ "closure" : "%s", "drivers" : [ { "name" : "sd_test", "code" : "%s" } ] }' % (base64.b64encode( closure_str ), base64.b64encode( driver_str ) )

# debugging entry point.
def test( ignore1, args ):

   # range of UGs to create
   start_idx = 0
   end_idx = len(nodes)

   # form flags
   do_ugs = False                # create default UG records (form argument: do_ugs=1) owned by the accompanied User identified by username
   do_rgs = False                # create default RG records (form argument: do_ugs=1) 
   do_ags = False                # create default AG records (form argument: do_ugs=1) 

   do_init = False               # create all default users and test volumes (form argument: do_init=1)
   reset_volume = False          # reset a volume's records (form argument: reset_volume=...)
   reset_volume_name = None      # volume to reset (taken from reset_volume=...)
   username = None               # email address of a valid User
   ug_name = None                # name of a UG to be manipulated
   ug_action = None              # what to do with a UG?  add, delete
   ug_host = None                # UG host
   ug_port = 32780               # UG port

   user = None
   volume = None
   localhost_name = None
   num_local = 0

   logging.info("args = %s" % args)
   
   
   if args != None:
      if args.has_key('start') and args.has_key('end'):
         # change the range of UGs to create
         start_idx = int(args['start'])
         end_idx = min( int(args['end']), len(nodes) )
         logging.info("start_idx = %s, end_idx = %s" % (start_idx, end_idx) )
         
      if args.has_key('do_ugs'):
         # create UGs from start_idx to end_idx
         do_ugs = True
         logging.info("do_ugs = True")
         
      if args.has_key('do_rgs'):
         # create UGs from start_idx to end_idx
         do_rgs = True
         logging.info("do_rgs = True")
         
      if args.has_key('do_ags'):
         # create UGs from start_idx to end_idx
         do_ags = True
         logging.info("do_ags = True")
         
      if args.has_key('do_init'):
         # create all Gs and users
         do_init = True
         logging.info("do_init = True")
         
      if args.has_key('do_local_ug'):
         # only create a UG for localhost (good for local debugging on the dev_server)
         start_idx = len(nodes) - 1
         end_idx = len(nodes)
         do_ugs = True
         logging.info("do_local_ug = True")
         
      if args.has_key('do_local_rg'):
         # create an RG for localhost in MS records
         start_idx = len(nodes) - 1
         end_idx = len(nodes)
         do_rgs = True
         logging.info("do_local_rg = True")
         
      if args.has_key('do_local_ag'):
         start_idx = len(nodes) - 1
         end_idx = len(nodes)
         # create an AG for localhost in MS records
         do_ags = True
         logging.info("do_local_ag = True")

      if args.has_key('reset_volume'):
         # delete all MSEntry records for a volume, and recreate the root.
         reset_volume = True
         reset_volume_name=args['reset_volume']
         logging.info("reset_volume = True")
         
      if args.has_key('username'):
         username = args['username']
         logging.info("username = %s" % username)
         
      if args.has_key('ug_name'):
         ug_name = args['ug_name']
         logging.info("ug_name = %s" % ug_name)
         
      if args.has_key('ug_action'):
         ug_action = args['ug_action']
         logging.info("ug_action = %s" % ug_action )
         
      if args.has_key('ug_host'):
         ug_host = args['ug_host']
         logging.info("ug_host = %s" % ug_host )
      
      if args.has_key('localhost'):
         nodes.remove("localhost")
         nodes.append( args['localhost'] )
         localhost_name = args['localhost']
         logging.info("localhost = %s" % args['localhost'])
         
      if args.has_key('num_local'):
         num_local = int( args['num_local'] )
         
         
      if args.has_key('ug_port'):
         try:
            ug_port = int(args['ug_port'])
         except:
            return (500, "Invalid argument value for ug_port")

         logging.info("ug_port = %s" % ug_port)

   if num_local != 0:
      if localhost_name == None:
         localhost_name = nodes[-1]
      
      for i in xrange(0,num_local):
         nodes.append( localhost_name )
      
      end_idx += num_local
      
      
   if reset_volume:

      volume = None
      try:
         volume = storage.read_volume( reset_volume_name )
      except:
         return (500, "Volume '%s' does not exist" % reset_volume_name)
      
      # delete all msentries 
      all_msentry = MSEntry.query( MSEntry.volume_id == volume.volume_id )
      all_msentry_keys_fut = all_msentry.fetch_async( None, keys_only=True, batch_size=1000 )

      # delete all msentryshards
      all_shard = MSEntryShard.query( MSEntryShard.msentry_volume_id == volume.volume_id )
      all_shard_keys_fut = all_shard.fetch_async( None, keys_only=True, batch_size=1000 )

      all_msentry_keys = all_msentry_keys_fut.get_result()
      all_shard_keys = all_shard_keys_fut.get_result()

      ndb.delete_multi( all_shard_keys + all_msentry_keys )

      now_sec, now_nsec = storage.clock_gettime()

      # create a root, with some sane defaults
      rc = storage.make_root( volume, user.owner_id )
                              

   
   volumes = []
   archive_volumes = []
   
   if do_init:
      # create users and make them all volumes
      for i, user_email in enumerate(users):
         try:
            user_key = storage.create_user( email=user_email, openid_url="https://vicci.org/id/%s" % user_email)
            user = user_key.get()
         except:
            logging.info( "traceback: " + traceback.format_exc() )
            try:
               user = storage.read_user( email=user_email )
            except:
               logging.info( "traceback: " + traceback.format_exc() )
               return (500, "Failed to read user '%s'" % user_email)

         test_volume_name = testvolume_name( user_email )
         
         try:
            # volume name: testvolume-$name
            logging.info("create %s" % test_volume_name )
            vol_public_key = volume_keys[i]["public"].strip()
            vol_private_key = volume_keys[i]["private"].strip()
            
            volume_key = storage.create_volume( user.email, name=test_volume_name, description="%s's test volume" % user_email,
                                               blocksize=61440, active=False, private=False, owner_id=i+1, volume_secret="abcdef", public_key=vol_public_key, private_key=vol_private_key )
            
            volume = volume_key.get()
            volumes.append( volume )
            
            logging.info("update %s" % test_volume_name )
            storage.update_volume( volume.volume_id, active=True )
            
            archive_public_key = volume_archive_keys[i]["public"].strip()
            archive_private_key = volume_archive_keys[i]["private"].strip()
            
            logging.info("create %s" % (test_volume_name + "-archive"))
            volume_key = storage.create_volume( user.email, name=(test_volume_name + "-archive"), description="%s's archive volume" % user_email,
                                                blocksize=61440, active=True, private=False, archive=True, owner_id=i+1, volume_secret="abcdef", public_key=archive_public_key, private_key=archive_private_key )
            archive_volume = volume_key.get()
            archive_volumes.append( archive_volume )
            
            
         except:
            logging.info( "traceback: " + traceback.format_exc() )
            try:
               volume = storage.get_volume_by_name( test_volume_name )
            except:
               logging.info( "traceback: " + traceback.format_exc() )
               return (500, "Failed to read volume '%s'" % test_volume_name)

         # assign volume to user
         if volume.volume_id not in user.volumes_o:
            user.volumes_o.append( volume.volume_id )
            user.put()
         if volume.volume_id not in user.volumes_rw:
            user.volumes_rw.append( volume.volume_id )
            user.put()
            
         if archive_volume.volume_id not in user.volumes_o:
            user.volumes_o.append( archive_volume.volume_id )
            user.put()
         if archive_volume.volume_id not in user.volumes_rw:
            user.volumes_rw.append( archive_volume.volume_id )
            user.put()

         # create a root MSEntry, with some sane defaults
         rc = storage.make_root( volume, user.owner_id )

      logging.info("created Volumes %s" % [x.volume_id for x in volumes] )
      logging.info("created Archive Volumes %s" % [x.volume_id for x in archive_volumes] )

   if do_ags:
      user = get_user( username )
      if user == None:
         return (500, "Invalid username %s" % username)
      
      for i in xrange(start_idx, end_idx):
         node = nodes[i]
         for j in xrange(0, len(archive_volumes)):
            archive_volume = archive_volumes[j]
            try:
               name = G_name("AG", node) + "-" + str(j) + "-" + str(i)
               port = 12780 + (i * (end_idx - start_idx + 1)) + j
               gw_key = storage.create_acquisition_gateway(user, archive_volume, ms_username=name, ms_password="sniff", host=node, port=port )
               logging.info("Created AG %s on port %d" % (name, port))
               
            except:
               logging.info("traceback: %s" % traceback.format_exc())
               # possibly already exists
               pass

   if do_rgs:
      user = get_user( username )
      if user == None:
         return (500, "Invalid username %s" % username)
      
      for i in xrange(start_idx, end_idx):
         node = nodes[i]
         for j in xrange(0, len(volumes)):
            volume = volumes[j]
            
            try:
               name = G_name("RG", node) + "-" + str(j) + "-" + str(i)
               port = 22780 + (i * (end_idx - start_idx + 1)) + j
               gw_key = storage.create_replica_gateway( user, volume, ms_username=name, ms_password="sniff", host=node, port=port, private=False, config=json_str )
                  
               logging.info("Created RG %s on port %d" % (name, port) )
            except:
               logging.info("traceback: %s" % traceback.format_exc())
               # already exists
               pass 

   # create fake UGs?
   if do_ugs:
      user = get_user( username )
      if user == None:
         return (500, "Invalid username %s" % username)

      volume_name = testvolume_name( user.email )
      volume = storage.get_volume_by_name( volume_name )
      
      for i in xrange(start_idx, end_idx):
         node = nodes[i]
         try:
            name = G_name("UG", node) + "-" + str(0) + "-" + str(i)
            port = 32780 + i
            gw_key = storage.create_user_gateway( user, volume, ms_username=name, ms_password="sniff", host=node, port=port, read_write=True )
               
            logging.info("Created UG %s on port %d" % (name, port))
         except:
            logging.info("traceback: %s" % traceback.format_exc())
            # already exists
            pass

   # UG action?
   if ug_action:
      if ug_name == None:
         return (500, "Missing argument: ug_name")

      user = get_user( username )
      if user == None:
         return (500, "Invalid username %s" % username)

      volume_name = testvolume_name( user.email )
      volume = storage.get_volume_by_name( volume_name )

      if ug_action == "create":

         if ug_host == None:
               return (500, "Missing argument: ug_host")

         try:
            storage.create_user_gateway( user, volume, ms_username=ug_name, ms_password="sniff", host=ug_host, port=str(ug_port) )
         except:
            return (500, "User gateway '%s' already exists" % ug_name)

            
      elif ug_action == "delete":
         try:
            ug = storage.read_user_gateway( ug_name )
            if ug.owner_id != user.owner_id:
               return (403, "User '%s' does not own UG '%s'" % (username, ug_name))
               
            storage.delete_user_gateway( ug_name )
         except:
            logging.info("traceback: %s" % traceback.format_exc())
            return (500, "User gateway '%s' does not exist" % ug_name )
         
      else:
         return (500, "ug_action '%s' not supported" % ug_action)
      
            
   return (200, "OK")

