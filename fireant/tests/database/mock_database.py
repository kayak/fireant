from fireant.database.vertica import VerticaDatabase


class MockDatabase(VerticaDatabase):
    # Vertica client that uses the vertica_python driver.

    def connect(self):
        pass
