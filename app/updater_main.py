from services.instances import graph_db
from updater.main import MainUpdateOrchestrator

if __name__ == "__main__":
    instance = MainUpdateOrchestrator(graph_db=graph_db)
    instance.start()
