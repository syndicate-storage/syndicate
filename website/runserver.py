#!/usr/bin/env python

import os
from syndicate_website import app
from config import SERVER_ADDRESS, SERVER_PORT 

def runserver():
	port = int(os.environ.get('PORT', SERVER_PORT))
	app.run(host=SERVER_ADDRESS, port=port, debug=True)

if __name__ == '__main__':
	runserver()
