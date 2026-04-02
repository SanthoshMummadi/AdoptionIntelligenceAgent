from collections import deque


class ConversationContext:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.last_account = None
        self.last_cloud = "Commerce Cloud"
        self.last_attrition = {}
        self.unused_features = []
        self.conversation_history = deque(maxlen=10)

    def add_message(self, role: str, content: str):
        self.conversation_history.append({"role": role, "content": content})

    def reset(self):
        self.last_account = None
        self.last_attrition = {}
        self.unused_features = []
        self.conversation_history.clear()


_contexts = {}


def get_or_create_context(user_id: str) -> ConversationContext:
    if user_id not in _contexts:
        _contexts[user_id] = ConversationContext(user_id)
    return _contexts[user_id]


def clear_context(user_id: str):
    if user_id in _contexts:
        _contexts[user_id].reset()
