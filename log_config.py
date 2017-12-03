
def get_config(script_name, webhook_url):
    log_path = '{}.log'.format(script_name)
    log_setting = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "simple",
                "stream": "ext://sys.stdout"
            },
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'simple',
                'filename': log_path,
                'maxBytes': 50 * 1024 * 1024,
                'backupCount': 3
            },
            'slack_handler': {
                "class": "slack_log_handler.SlackLogHandler",
                'webhook_url': webhook_url,
                "level": "INFO",
                "formatter": "simple",
            },
        },
        "loggers": {
            '': {
                "level": "DEBUG",
                "handlers": ["console", 'file', 'slack_handler']
            }
        },

    }
    return log_setting