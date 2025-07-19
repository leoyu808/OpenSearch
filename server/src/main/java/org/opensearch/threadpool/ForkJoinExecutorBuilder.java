/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.Node;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;


/**
 * A builder that creates a ForkJoinPool–based executor for parallel KNN tasks.
 */
public class ForkJoinExecutorBuilder extends ExecutorBuilder<ForkJoinExecutorBuilder.ForkJoinExecutorSettings> {
    private final Setting<Integer> sizeSetting;

    /**
     * Construct a ForkJoin executor builder.
     *
     * @param settings   the node-level settings
     * @param name       the name of the executor
     * @param size       the fixed number of threads
     */
    public ForkJoinExecutorBuilder(final Settings settings, final String name, final int size) {
        this(settings, name, size, "thread_pool." + name, false);
    }

    /**
     * Construct a ForkJoin executor builder.
     *
     * @param settings   the node-level settings
     * @param name       the name of the executor
     * @param size       the fixed number of threads
     * @param prefix     the prefix for the settings keys
     */
    public ForkJoinExecutorBuilder(final Settings settings, final String name, final int size, final String prefix) {
        this(settings, name, size, prefix, false);
    }

    /**
     * Construct a ForkJoin executor builder.
     *
     * @param settings   the node-level settings
     * @param name       the name of the executor
     * @param size       the fixed number of threads
     * @param prefix     the prefix for the settings keys
     * @param deprecated whether or not the thread pool is deprecated
     */
    public ForkJoinExecutorBuilder(
        final Settings settings,
        final String name,
        final int size,
        final String prefix,
        final boolean deprecated
    ) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        final Setting.Property[] properties;
        if (deprecated) {
            properties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Deprecated };
        } else {
            properties = new Setting.Property[] { Setting.Property.NodeScope };
        }
        this.sizeSetting = new Setting<>(
            sizeKey,
            s -> Integer.toString(size),
            s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
            properties
        );
        final String queueSizeKey = settingsKey(prefix, "queue_size");
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Collections.singletonList(sizeSetting);
    }

    @Override
    ForkJoinExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int size = sizeSetting.get(settings);
        return new ForkJoinExecutorSettings(nodeName, size);
    }

    ThreadPool.ExecutorHolder build(final ForkJoinExecutorBuilder.ForkJoinExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        final ExecutorService executor = new ForkJoinPool(
            size
        );
        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FORKJOIN,
            size,
            size,
            null,
            null
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize()
        );
    }

    static class ForkJoinExecutorSettings extends ExecutorBuilder.ExecutorSettings {
        final int size;

        ForkJoinExecutorSettings(String nodeName, int size) {
            super(nodeName);
            this.size = size;
        }
    }

}
