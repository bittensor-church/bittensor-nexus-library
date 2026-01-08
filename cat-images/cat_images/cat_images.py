import time

from nexus.core.runtime.actor import Actor
from nexus.core.runtime.event_bus import EventBus
from nexus.core.runtime.events import PipeToBus, StopBusEvent

from .validator import make_subnet

if __name__ == "__main__":
    piping, nodes = make_subnet()

    pipe_to_bus = PipeToBus()
    actors: list[Actor] = [node.build_actor(pipe_to_bus=pipe_to_bus) for node in nodes]

    event_bus = EventBus(piping.pipes, pipe_to_bus, actors)

    for actor in actors:
        actor.run_loop()

    event_bus.run_loop()

    print("Event bus and actors are running. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pipe_to_bus.put(StopBusEvent())

