# Patterns and Recipes

## Feed metagraph snapshots from a beat

Use a beat or any other typed trigger to drive `MetagraphSource` before downstream routing or scoring steps. This lets the validator refresh its subnet view on a schedule and then pass the resulting metagraph snapshot into later graph stages.

```python
beat = EpochBeatNode("epoch-beat")
metagraph = MetagraphSource("metagraph")

Flow.from_connectable(beat.source).then(metagraph.trigger)
```
