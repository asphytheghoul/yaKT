class RecordManager:
   def __init__(self, database):
       self.database = database

   def create_record(self, record_id, record):
       self.database.add_record(record_id, record)

   def read_record(self, record_id):
       return self.database.get_record(record_id)

   def delete_record(self, record_id):
       self.database.delete_record(record_id)
