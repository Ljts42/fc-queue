import java.util.concurrent.*
import java.util.concurrent.atomic.*

/**
 * @author Sentemov Lev
 */
class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = AtomicBoolean(false) // unlocked initially
    private val tasksForCombiner = AtomicReferenceArray<Any?>(TASKS_FOR_COMBINER_SIZE)

    private fun tryLock() = combinerLock.compareAndSet(false, true)

    private fun unlock() { combinerLock.set(false) }

    private fun combine() {
        for (i in 0 until TASKS_FOR_COMBINER_SIZE) {
            val task = tasksForCombiner.get(i) ?: continue
            if (task is Dequeue) {
                tasksForCombiner.set(i, Result(queue.removeFirstOrNull()))
            } else if (task !is Result<*>) {
                queue.addLast(task as E)
                tasksForCombiner.set(i, Result(task as E))
            }
        }
        unlock()
    }

    override fun enqueue(element: E) {
        var cellIndex = randomCellIndex()
        var waiting = false
        while (true) {
            if (waiting) {
                val res = tasksForCombiner.get(cellIndex)
                if (res is Result<*>) {
                    tasksForCombiner.set(cellIndex, null)
                    return
                }
            }
            if (tryLock()) {
                if (waiting) {
                    val task = tasksForCombiner.get(cellIndex)
                    tasksForCombiner.set(cellIndex, null)
                    if (task is Result<*>) {
                        combine()
                        return
                    }
                }
                queue.addLast(element)
                combine()
                return
            }
            if (tasksForCombiner.compareAndSet(cellIndex, null, element)) {
                waiting = true
            }
        }
    }

    override fun dequeue(): E? {
        var cellIndex = randomCellIndex()
        var waiting = false
        while (true) {
            if (tryLock()) {
                if (waiting) {
                    val task = tasksForCombiner.get(cellIndex)
                    tasksForCombiner.set(cellIndex, null)
                    if (task is Result<*>) {
                        val res = task.value as E
                        combine()
                        return res
                    }
                }
                val result = queue.removeFirstOrNull()
                combine()
                return result
            }
            if (waiting) {
                val res = tasksForCombiner.get(cellIndex)
                if (res is Result<*>) {
                    tasksForCombiner.set(cellIndex, null)
                    return res.value as E
                }
            }
            if (tasksForCombiner.compareAndSet(cellIndex, null, Dequeue)) {
                waiting = true
            }
        }
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(tasksForCombiner.length())
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

private object Dequeue

private class Result<V>(
    val value: V
)