import webapp2
from openid.gaesession import delete_expired_sessions
import logging

class CronRequestHandler(webapp2.RequestHandler):
   	"""
   	Cron job request handler.
	"""
  	def get( self ):
	  	logging.info("Cron jobs triggered")
      	while not delete_expired_sessions():
          	pass

app = webapp2.WSGIApplication([
	(r'/cron.*', CronRequestHandler),
], debug=True)