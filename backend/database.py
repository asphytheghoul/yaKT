class Database:
   def __init__(self):
       self.records = {}

   def add_record(self, record_id, record):
       self.records[record_id] = record

   def get_record(self, record_id):
       return self.records.get(record_id)

   def delete_record(self, record_id):
       if record_id in self.records:
           del self.records[record_id]
