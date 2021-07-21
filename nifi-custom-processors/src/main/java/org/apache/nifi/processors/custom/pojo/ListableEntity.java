package org.apache.nifi.processors.custom.pojo;

public interface ListableEntity {

    /**
     * @return The name of the remote entity
     */
    String getName();

    /**
     * @return the identifier of the remote entity. This may or may not be the same as the name of the
     *         entity but should be unique across all entities.
     */
    String getIdentifier();


    /**
     * @return the timestamp for this entity so that we can be efficient about not performing listings of the same
     *         entities multiple times
     */
    long getTimestamp();

}

