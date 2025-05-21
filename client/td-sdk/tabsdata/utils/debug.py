#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import time
from io import BytesIO

import pydevd_pycharm
import pygame
from gtts import gTTS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REMOTE_DEBUG = "TD_REMOTE_DEBUG"
REMOTE_DEBUG_HOST = "127.0.0.1"
REMOTE_DEBUG_PORT = 5678

TRUE_1 = "1"
TRUE_TRUE = "true"
TRUE_YES = "yes"
TRUE_Y = "y"
TRUE_ON = "on"

FALSE_0 = "0"
FALSE_FALSE = "false"
FALSE_NO = "no"
FALSE_N = "n"
FALSE_OFF = "off"

TRUE_VALUES = {TRUE_1, TRUE_TRUE, TRUE_YES, TRUE_Y, TRUE_ON}
FALSE_VALUES = {FALSE_0, FALSE_FALSE, FALSE_NO, FALSE_N, FALSE_OFF}


def notification(text, language="en"):
    pygame.mixer.init()
    mp3_fp = BytesIO()
    tts = gTTS(text, lang=language)
    tts.write_to_fp(mp3_fp)
    mp3_fp.seek(0)
    pygame.mixer.music.load(mp3_fp, "mp3")
    pygame.mixer.music.play()
    while pygame.mixer.music.get_busy():
        time.sleep(0.1)


def remote_debug(force: bool = False) -> bool:
    remote_debug_enabled = os.getenv(REMOTE_DEBUG, FALSE_FALSE).lower() in TRUE_VALUES
    if remote_debug_enabled or force:
        # noinspection PyBroadException
        try:
            pydevd_pycharm.settrace(
                host=REMOTE_DEBUG_HOST,
                port=REMOTE_DEBUG_PORT,
                suspend=False,
                stdoutToServer=True,
                stderrToServer=True,
            )
            message = "Execution suspended. Continue in your debug tool"
            logger.info("Remote debug enabled...")
            notification(message, language="en")
            breakpoint()
            return True
        except Exception as e:
            message = "Error connecting to remote debugger. Check your debug tool"
            logger.error(message)
            logger.error(f"Error: {e}")
            notification(message, language="en")
            return False
    return False
