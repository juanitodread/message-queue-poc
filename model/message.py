import datetime


class Message:
    def __init__(self, bot_id, user_id, type, content):
        self._bot_id = bot_id
        self._user_id = user_id
        self._type = type
        self._content = content
        self._timestamp = datetime.datetime.utcnow()

    @property
    def bot_id(self):
        return self._bot_id

    @property
    def user_id(self):
        return self._user_id

    @property
    def type(self):
        return self._type

    @property
    def content(self):
        return self._content

    @property
    def timestamp(self):
        return self._timestamp
