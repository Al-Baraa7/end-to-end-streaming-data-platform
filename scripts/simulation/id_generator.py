"""IDGenerator — Phase 2: Identity & Randomness

UUID7 (time-ordered) generator for collision-resistant entity IDs.
See README.md#phase-markers-used-in-code-comments
"""

from uuid6 import uuid7


class IDGenerator:

    @staticmethod
    def generate() -> str:
        return str(uuid7())

# from scripts.simulation.id_generator import IDGenerator


if __name__ == "__main__":
    # Manual smoke check only: `py -3 -m scripts.simulation.id_generator`.
    # These used to run at import time and polluted the stdout of every consumer
    # (D5 in the blueprint's drift ledger).
    id_1 = IDGenerator.generate()
    id_2 = IDGenerator.generate()
    id_3 = IDGenerator.generate()

    print(id_1)
    print(id_2)
    print(id_3)

    print(id_1 != id_2 != id_3)