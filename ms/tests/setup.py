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
         
      if args.has_key('ug_port'):
         try:
            ug_port = int(args['ug_port'])
         except:
            return (500, "Invalid argument value for ug_port")

         logging.info("ug_port = %s" % ug_port)

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
            volume_key = storage.create_volume( user.email, name=test_volume_name, description="%s's test volume" % user_email, blocksize=61440, active=True, private=False, owner_id=i+1, volume_secret="abcdef" )
            volume = volume_key.get()
            volumes.append( volume )
            
            volume_key = storage.create_volume( user.email, name=(test_volume_name + "-archive"), description="%s's archive volume" % user_email,
                                                blocksize=61440, active=True, private=False, archive=True, owner_id=i+1, volume_secret="abcdef" )
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
            archive_volume = archive_volume[j]
            try:
               gw_key = storage.create_acquisition_gateway(user, ms_username=G_name("AG", node) + "-" + str(archive_volume.volume_id), ms_password="sniff", host=node, port=32778, volume=archive_volume )
               logging.info("Created AG %s" % G_name("AG", node) + "-" + str(archive_volume.volume_id))
               
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
         for j in xrange(0, len(volume_ids)):
            volume = volumes[j]
            
            try:
               gw_key = storage.create_replica_gateway( user, ms_username=G_name("RG", node) + "-" + str(volume.volume_id), ms_password="sniff", volume=volume, host=node, port=32779, private=False, config="""{ "closure" : "CiMhL3Vzci9iaW4vZW52IHB5dGhvbiAKCkNPTkZJRyA9IHsnZm9vJzogJ2Jhcid9CgpkZWYgcmVwbGljYV9yZWFkKCBkcml2ZXJzLCByZXF1ZXN0X2luZm8sIGZpbGVuYW1lLCBvdXRmaWxlICk6CiAgIHByaW50ICJyZXBsaWNhX3JlYWQgY2FsbGVkISIKICAgCiAgIGdsb2JhbCBDT05GSUcgCiAgIAogICBwcmludCAiQ09ORklHID0gIiArIHN0cihDT05GSUcpCiAgIHByaW50ICJkcml2ZXJzID0gIiArIHN0cihkcml2ZXJzKQogICBwcmludCAicmVxdWVzdF9pbmZvID0gIiArIHN0cihyZXF1ZXN0X2luZm8pCiAgIHByaW50ICJmaWxlbmFtZSA9ICIgKyBzdHIoZmlsZW5hbWUpCiAgIHByaW50ICJvdXRmaWxlID0gIiArIHN0cihvdXRmaWxlKQogICBwcmludCAiIgogICAKICAgZHJpdmVyc1snc2RfdGVzdCddLnJlYWRfZmlsZSggZmlsZW5hbWUsIG91dGZpbGUsIGV4dHJhX3BhcmFtPSJGb28iICkKICAgCiAgIHJldHVybiAwCiAgIApkZWYgcmVwbGljYV93cml0ZSggZHJpdmVycywgcmVxdWVzdF9pbmZvLCBmaWxlbmFtZSwgaW5maWxlICk6CiAgIHByaW50ICJyZXBsaWNhX3dyaXRlIGNhbGxlZCEiCiAgIAogICBnbG9iYWwgQ09ORklHIAogICAKICAgcHJpbnQgIkNPTkZJRyA9ICIgKyBzdHIoQ09ORklHKQogICAKICAgcHJpbnQgImRyaXZlcnMgPSAiICsgc3RyKGRyaXZlcnMpCiAgIHByaW50ICJyZXF1ZXN0X2luZm8gPSAiICsgc3RyKHJlcXVlc3RfaW5mbykKICAgcHJpbnQgImZpbGVuYW1lID0gIiArIHN0cihmaWxlbmFtZSkKICAgcHJpbnQgImluZmlsZSA9ICIgKyBzdHIoaW5maWxlKQogICBwcmludCAiIgogICAKICAgZHJpdmVyc1snc2RfdGVzdCddLndyaXRlX2ZpbGUoIGZpbGVuYW1lLCBpbmZpbGUsIGV4dHJhX3BhcmFtPSJGb28iICkKICAgCiAgIHJldHVybiAwCiAgIAo=", "drivers" : [ { "name" : "sd_test", "code" : "CiMhL3Vzci9iaW4vZW52IHB5dGhvbiAKCmRlZiByZWFkX2ZpbGUoIGZpbGVuYW1lLCBvdXRmaWxlLCAqKmt3ICk6CiAgIHByaW50ICIgIHJlYWRfZmlsZSBjYWxsZWQhIgogICBwcmludCAiICBmaWxlbmFtZSA9ICIgKyBzdHIoZmlsZW5hbWUpCiAgIHByaW50ICIgIG91dGZpbGUgPSAiICsgc3RyKG91dGZpbGUpCiAgIHByaW50ICIgIGt3ID0gIiArIHN0cihrdykKICAgcHJpbnQgIiIKICAgCiAgIHJldHVybiAwCgpkZWYgd3JpdGVfZmlsZSggZmlsZW5hbWUsIGluZmlsZSwgKiprdyApOgogICBwcmludCAiICB3cml0ZV9maWxlIGNhbGxlZCEiCiAgIHByaW50ICIgIGZpbGVuYW1lID0gIiArIHN0cihmaWxlbmFtZSkKICAgcHJpbnQgIiAgaW5maWxlID0gIiArIHN0cihpbmZpbGUpCiAgIHByaW50ICIgIGt3ID0gIiArIHN0cihrdykKICAgcHJpbnQgIiIKICAgCiAgIHJldHVybiAwCgogICAK" } ] }""" )
               
                  
               logging.info("Created RG %s" % G_name("RG", node) + "-" + str(volume.volume_id))
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
            gw_key = storage.create_user_gateway( user, volume, ms_username=G_name("UG", node), ms_password="sniff", host=node, port=32780, read_write=True )
               
            logging.info("Created UG %s" % G_name("UG", node))
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

