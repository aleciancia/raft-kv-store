import json
import os

class StorageModule:
    def __init__(self, node_id):
        self.node_id = node_id
        self.db = {}
        self.log_file = f"{self.node_id}_wal.log"
        self._recover()

    def _recover(self):
        if not os.path.exists(self.log_file):
            return
            
        with open(self.log_file, 'r') as f:
            for line in f:
                entry = json.loads(line.strip())
                if entry['action'] == 'SET':
                    self.db[entry['key']] = entry['value']
                elif entry['action'] == 'DELETE':
                    self.db.pop(entry['key'], None)

    def set_value(self, key, value):
        entry = {'action': 'SET', 'key': key, 'value': value}
        self._append_to_log(entry)
        self.db[key] = value

    def get_value(self, key):
        return self.db.get(key)

    def delete_value(self, key):
        entry = {'action': 'DELETE', 'key': key}
        self._append_to_log(entry)
        self.db.pop(key, None)

    def _append_to_log(self, entry):
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')
            f.flush()
            os.fsync(f.fileno())