class SqlQueryLoader:
    def __init__(self, file_path):
        self.file_path = file_path

    def load(self):
        with open(self.file_path, "r") as file:
            return file.read()

    def replace_placeholders(self, query, replacements):
        for placeholder, replacement in replacements.items():
            query = query.replace(placeholder, replacement)
        return query
