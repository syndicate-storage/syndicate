
PAPER := usenix-atc2014
all: paper.pdf

paper.pdf: paper.dvi
	dvipdf $(PAPER).dvi

paper.dvi:
	/usr/bin/latex $(PAPER).tex
	/usr/bin/bibtex $(PAPER)
	/usr/bin/latex $(PAPER).tex
	/usr/bin/latex $(PAPER).tex

osx:
	/usr/texbin/pdflatex $(PAPER).tex

osx-bib:
	/usr/texbin/latex $(PAPER).tex
	/usr/texbin/bibtex $(PAPER)
	/usr/texbin/latex $(PAPER).tex
	/usr/texbin/latex $(PAPER).tex
	/usr/texbin/pdflatex $(PAPER).tex

clean:
	rm -f $(PAPER).pdf $(PAPER).dvi $(PAPER).aux $(PAPER).bbl $(PAPER).blg $(PAPER).log $(PAPER).ent

