package org.qcri.rheem.core.plan;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction,
 * i.e., outer output slot -> inner output slot, inner input slot -> outer input slot).
 */
public class SlotMapping {

    private final Map<Slot, Slot> upstreamMapping = new HashMap<>();

    private Map<Slot, Collection> downstreamMapping = null;

    public static SlotMapping wrap(Operator wrappee, Operator wrapper) {
        SlotMapping slotMapping = new SlotMapping();
        slotMapping.mapAllUpsteam(wrapper.getAllOutputs(), wrappee.getAllOutputs());
        slotMapping.mapAllUpsteam(wrappee.getAllInputs(), wrapper.getAllInputs());
        return slotMapping;
    }

    public void mapAllUpsteam(InputSlot[] sources, InputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            mapUpstream(sources[i], targets[i]);
        }
    }

    public void mapAllUpsteam(OutputSlot[] sources, OutputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            mapUpstream(sources[i], targets[i]);
        }
    }

    public void mapUpstream(InputSlot<?> source, InputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.upstreamMapping.put(source, target);
        this.downstreamMapping = null;
    }

    public void mapUpstream(OutputSlot<?> source, OutputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException(String.format("Incompatible slots given: %s -> %s", source, target));
        }

        this.upstreamMapping.put(source, target);
        this.downstreamMapping = null;
    }

    public <T> InputSlot<T> resolveUpstream(InputSlot<T> source) {
        return (InputSlot<T>) this.upstreamMapping.get(source);
    }

    public <T> OutputSlot<T> resolveUpstream(OutputSlot<T> source) {
        return (OutputSlot<T>) this.upstreamMapping.get(source);
    }

    public <T> Collection<InputSlot<T>> resolveDownstream(InputSlot<T> source) {
        return (Collection<InputSlot<T>>) getOrCreateDownstreamMapping().getOrDefault(source, Collections.emptyList());
    }

    public <T> Collection<OutputSlot<T>> resolveDownstream(OutputSlot<T> source) {
        return (Collection<OutputSlot<T>>) getOrCreateDownstreamMapping().getOrDefault(source, Collections.emptyList());
    }

    /**
     * Retrieves {@link #downstreamMapping} or creates it if it does not exist.
     *
     * @return {@link #downstreamMapping}
     */
    private Map<Slot, Collection> getOrCreateDownstreamMapping() {
        if (this.downstreamMapping == null) {
            this.downstreamMapping = this.upstreamMapping.entrySet().stream().collect(
                    Collectors.groupingBy(
                            Map.Entry::getValue,
                            Collectors.mapping(
                                    Map.Entry::getKey,
                                    Collectors.toCollection(LinkedList::new))));
        }

        return this.downstreamMapping;
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     *
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    public void replaceInputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getNumInputs() != newOperator.getNumInputs()) {
            throw new IllegalArgumentException("Operators are not matching.");
        }

        for (int i = 0; i < oldOperator.getNumInputs(); i++) {
            final InputSlot<?> oldInput = oldOperator.getInput(i);
            final InputSlot<?> newInput = newOperator.getInput(i);

            final InputSlot<?> outerInput = resolveUpstream(oldInput);
            if (outerInput != null) {
                mapUpstream(newInput, outerInput);
                delete(oldInput);
            }
        }
    }

    /**
     * Removes an existing mapping.
     *
     * @param key the key of the mapping to remove
     */
    private void delete(InputSlot<?> key) {
        this.upstreamMapping.remove(key);
    }

    /**
     * Removes an existing mapping.
     *
     * @param key the key of the mapping to remove
     */
    private void delete(OutputSlot<?> key) {
        this.upstreamMapping.remove(key);
    }

    /**
     * Replace the mappings from an old, wrapped operator with a new wrapped operator.
     *
     * @param oldOperator the old wrapped operator
     * @param newOperator the new wrapped operator
     */
    public void replaceOutputSlotMappings(Operator oldOperator, Operator newOperator) {
        if (oldOperator.getNumOutputs() != newOperator.getNumOutputs()) {
            throw new IllegalArgumentException("Operators are not matching.");
        }

        for (int i = 0; i < oldOperator.getNumOutputs(); i++) {
            final OutputSlot<?> oldOutput = oldOperator.getOutput(i);
            final OutputSlot<?> newOutput = newOperator.getOutput(i);

            this.upstreamMapping.entrySet().stream()
                    .filter(entry -> entry.getValue() == oldOutput)
                    .findFirst()
                    .map(Map.Entry::getKey)
                    .ifPresent(outerOutput -> this.mapUpstream((OutputSlot<?>) outerOutput, newOutput));
            // No need for delete as we are replacing the old mapping.
        }
    }
}
