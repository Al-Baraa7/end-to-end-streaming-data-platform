# scripts/simulation/id_generator.py

from uuid6 import uuid7


class IDGenerator:

    @staticmethod
    def generate() -> str:
        return str(uuid7())

#from scripts.simulation.id_generator import IDGenerator


id_1 = IDGenerator.generate()
id_2 = IDGenerator.generate()
id_3 = IDGenerator.generate()

print(id_1)
print(id_2)
print(id_3)

print(id_1 != id_2 != id_3)