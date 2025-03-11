package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;


import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SolutionThread extends UserThread {
    // TODO: add fields here!
    private static final ConcurrentHashMap<MethodID, CompiledMethodInfo> cachedCompiledMethods = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MethodID, Long> hotness = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rwlMethods = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock rwlHotness = new ReentrantReadWriteLock();
    private final CompilationExecution multiThreadCompiler;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        // TODO: initialize fields!
        multiThreadCompiler = new CompilationExecution(compiler, compilationThreadBound);
    }


    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long hotLevel = getHotness(id);
        putHotness(id, hotLevel + 1);
        CompiledMethodInfo cachedInfo = getMethods(id);
        CompilationLevel level = null;

        if (hotLevel > 10_000 && cachedInfo != null && cachedInfo.level.ordinal() < CompilationLevel.L2.ordinal()) {
            level = CompilationLevel.L2;
        } else if (hotLevel > 5_000 && cachedInfo == null) {
            level = CompilationLevel.L1;
        }
        //lock.lock();
        //synchronized (in) {
        if (level != null) {
            CompiledMethod method = multiThreadCompiler.compile(id, level);
            putMethods(id, new CompiledMethodInfo(method, level));
            return exec.execute(method);
        }
        //}
        //lock.unlock();
        return cachedInfo != null
                ? exec.execute(cachedInfo.method)
                : exec.interpret(id);
    }

    // TODO: add methods
    // TODO: add inner classes
    // TODO: add utility classes in the same package
    private enum CompilationLevel {
        L1, L2
    }

    private Long getHotness(MethodID key) {
        rwlHotness.readLock().lock();
        try {
            Long v;
            return (v = hotness.get(key)) == null ? 0L : v;
        } finally {
            rwlHotness.readLock().unlock();
        }
    }

    private void putHotness(MethodID key, Long value) {
        rwlHotness.writeLock().lock();
        try {
            hotness.put(key, value);
        } finally {
            rwlHotness.writeLock().unlock();
        }
    }

    private CompiledMethodInfo getMethods(MethodID key) {
        rwlMethods.readLock().lock();
        try {
            return cachedCompiledMethods.get(key);
        } finally {
            rwlMethods.readLock().unlock();
        }
    }

    private void putMethods(MethodID key, CompiledMethodInfo value) {
        rwlMethods.writeLock().lock();
        try {
            cachedCompiledMethods.put(key, value);
        } finally {
            rwlMethods.writeLock().unlock();
        }
    }

    private static class CompiledMethodInfo {
        public final CompiledMethod method;
        public final CompilationLevel level;

        public CompiledMethodInfo(CompiledMethod method, CompilationLevel level) {
            this.method = method;
            this.level = level;

        }
    }


    private static class CompilationExecution {
        private final ExecutorService executor;
        private final CompilationEngine engine;

        private CompilationExecution(CompilationEngine engine, int compilationThreadBound) {
            this.engine = engine;
            executor = Executors.newFixedThreadPool(compilationThreadBound);
        }

        public CompiledMethod compile(MethodID id, CompilationLevel level) {
            Callable<CompiledMethod> compileCallable;
            if (level == CompilationLevel.L1) {
                compileCallable = () -> engine.compile_l1(id);
            } else {
                compileCallable = () -> engine.compile_l2(id);
            }
            Future<CompiledMethod> future = executor.submit(compileCallable);
            CompiledMethod code;
            try {
                code = future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return code;
        }
    }
}