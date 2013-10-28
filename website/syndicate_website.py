#!/usr/bin/env python

'''
	A bootstrap 3.0 based website for Syndicate 
'''


from flask import Flask, render_template, request
import flask 
app = Flask(__name__)

#-----------------------------------
@app.route('/')
def index():
	return render_template('index.html')

#-----------------------
if __name__ == '__main__':

	app.run(debug=True)
