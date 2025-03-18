package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;


import java.util.concurrent.*;

/***
 * Решение использует глобальное кэширование из 2-х кэшей для методов( метод - (уровень, скомп.метод))
 * и счётчик кол-ва вызовов метода. Они представлены ConcurrentHashMap и операции get, put безопасны.<br>
 * Описание решения:<br>
 * Увеличивается стётчик вызовов, проверяется кэш методов:<br>
 *  если >10_000 и уровень компиляции меньше L2, то происходит асинхронная компиляция до L2.<br>
 *  если >5_000 && <=10_000 и уровень меньше L1, то асинхронная компиляция до L1.<br>
 *  в противном случае метод остаётся интерпретируемым.<br>
 * Если метод есть в кэше он исполняется, иначе интерпретируется<br>
 * В процессе асинхронной компиляции происходит обновление кэша методов.<br>
 */
public class SolutionThread extends UserThread {
    // TODO: add fields here!
    private static final ConcurrentHashMap<MethodID, CompiledMethodInfo> cachedCompiledMethods = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MethodID, Long> hotness = new ConcurrentHashMap<>();
    private final CompilationExecution multiThreadCompiler;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        // TODO: initialize fields!
        multiThreadCompiler = new CompilationExecution(compiler, compilationThreadBound);
    }


    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long hotLevel = hotness.getOrDefault(id, 0L);
        hotness.put(id, hotLevel + 1);
        CompiledMethodInfo cachedInfo = cachedCompiledMethods.get(id);
        CompilationLevel level = null;

        if (hotLevel > 10_000 && cachedInfo != null && cachedInfo.level.ordinal() < CompilationLevel.L2.ordinal()) {
            level = CompilationLevel.L2;
        } else if (hotLevel > 5_000 && cachedInfo == null) {
            level = CompilationLevel.L1;
        }

        if (level != null) {
            multiThreadCompiler.asynCompile(id, level);
        }
        cachedInfo = cachedCompiledMethods.get(id);
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

        public void asynCompile(MethodID id, CompilationLevel level) {
            CompletableFuture.runAsync(() -> {
                try {
                    CompiledMethod method = (level == CompilationLevel.L1)
                            ? engine.compile_l1(id)
                            : engine.compile_l2(id);

                    cachedCompiledMethods.compute(id, (k, oldInfo) -> {
                        if (oldInfo == null || level.ordinal() > oldInfo.level.ordinal()) {
                            return new CompiledMethodInfo(method, level);
                        }
                        return oldInfo;
                    });

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);

        }
    }
}