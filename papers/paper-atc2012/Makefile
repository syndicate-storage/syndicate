
PAPER := usenix-atc2012
all: paper.pdf

paper.pdf: paper.dvi
	dvipdf $(PAPER).dvi

paper.dvi:
	/usr/bin/latex $(PAPER).tex
	/usr/bin/bibtex $(PAPER)
	/usr/bin/latex $(PAPER).tex
	/usr/bin/latex $(PAPER).tex

clean:
	rm -f $(PAPER).pdf $(PAPER).dvi $(PAPER).aux $(PAPER).bbl $(PAPER).blg $(PAPER).log $(PAPER).ent

