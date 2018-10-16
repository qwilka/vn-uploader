import json
import logging
import os
from string import Template
import sys
import uuid

logger = logging.getLogger(__name__)


def state2uuid(state):
    "Return UUID for the given state dictionary."
    if not isinstance(state, dict):
        logger.error("state2hash: arg «state» must be a dict: «%s»" %(state,))
        return False
    state_str = json.dumps(state, sort_keys=True, default=str)
    state_str = state_str.upper()
    return uuid.uuid3(uuid.NAMESPACE_URL, state_str).hex


def state2string(state, template="${country}-${field}-${domain}"):
    "Return string for the given template and state dictionary."
    if not isinstance(state, dict):
        logger.error("state2hash: arg «state» must be a dict: «%s»" %(state,))
        return False
    _temp = Template(template)
    _st = _temp.substitute(**state)
    return _st