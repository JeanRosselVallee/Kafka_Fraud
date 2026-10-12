import logging
import colorlog

# Logging


def setup_logging() -> None:
    """
    Set up logging configuration for the application:
    - a basic format handler for other modules
    - color format handler specifically for 'fraud_detection' logger
    """

    global_format = '%(log_color)s - %(asctime)s - %(message)s'
    other_apps_colors = {
        'DEBUG': 'cyan,bg_light_black',
        'INFO': 'green,bg_light_black',
        'WARNING': 'yellow,bg_light_black',
        'ERROR': 'red,bg_light_black'
    }
    custom_colors = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red'
    }

    # 1. Basic format handler for other modules
    other_apps_handler = logging.StreamHandler()
    other_apps_handler.setFormatter(colorlog.ColoredFormatter(
            global_format,
            log_colors=other_apps_colors
        )
    )
    logging.getLogger().addHandler(other_apps_handler)
    logging.getLogger().setLevel(logging.INFO)

    # 2. Color format handler for app fraud_detection
    custom_handler = colorlog.StreamHandler()
    custom_handler.setFormatter(colorlog.ColoredFormatter(
        global_format,
        log_colors=custom_colors
    ))

    custom_logger = logging.getLogger('fraud_detection')
    custom_logger.addHandler(custom_handler)
    custom_logger.setLevel(logging.DEBUG)
    custom_logger.propagate = False  # Prevent duplicate output to root logger
