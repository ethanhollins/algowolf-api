import os
from app import app

if __name__ == '__main__':
	app.run(port=3000)

	if app.config.get('ENV') == 'development':
		# Kill own process
		KILL_SIGNAL = 9
		os.kill(os.getpid(), KILL_SIGNAL)