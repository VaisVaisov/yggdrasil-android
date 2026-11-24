package eu.neilalexander.yggdrasil

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.Build
import android.util.Log
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

private const val TAG = "NetworkSwitch"

/** Coalesces the burst of network callbacks into one reconnect. */
private const val DEBOUNCE_MS = 1200L

/** Peer retry ticks, in milliseconds, relative to the debounced reconnect. */
private val RETRY_DELAYS_MS = longArrayOf(0, 2_000, 6_000, 15_000, 30_000)

/**
 * Watches the system default network and asks the tunnel to reconnect the
 * overlay peers and the exit node when the network changes.
 *
 * Android delivers a switch as a burst of callbacks, so events are coalesced
 * through a debounce timer and the reconnect itself runs on a dedicated
 * single-thread executor: never on a callback thread, never in parallel with
 * itself, and never while the network is down.
 */
class NetworkSwitchHandler(
    private val context: Context,
    private val isRunning: () -> Boolean,
    private val reconnect: (attempt: Int) -> Unit,
) {
    private val executor = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "network-switch").apply { isDaemon = true }
    }
    private val attempts = ArrayList<ScheduledFuture<*>>()
    private var pending: ScheduledFuture<*>? = null
    private var current: Network? = null
    private var down = false
    private var callback: ConnectivityManager.NetworkCallback? = null
    private var registered = false

    fun start() {
        if (registered) {
            return
        }
        val manager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val cb = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                Log.d(TAG, "network available")
                current = network
                down = false
                schedule()
            }

            override fun onLost(network: Network) {
                Log.d(TAG, "network lost")
                if (network == current) {
                    down = true
                }
            }

            override fun onCapabilitiesChanged(network: Network, caps: NetworkCapabilities) {
                if (network == current &&
                    !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                ) {
                    down = true
                }
            }
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            manager.registerDefaultNetworkCallback(cb)
        } else {
            val request = NetworkRequest.Builder()
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build()
            manager.registerNetworkCallback(request, cb)
        }
        callback = cb
        registered = true
    }

    fun stop() {
        if (!registered) {
            return
        }
        registered = false
        callback?.let {
            val manager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            try {
                manager.unregisterNetworkCallback(it)
            } catch (_: IllegalArgumentException) {
                // Already unregistered.
            }
        }
        callback = null
        pending?.cancel(false)
        pending = null
        executor.shutdownNow()
    }

    private fun schedule() {
        pending?.cancel(false)
        attempts.forEach { it.cancel(false) }
        attempts.clear()
        pending = executor.schedule({ run() }, DEBOUNCE_MS, TimeUnit.MILLISECONDS)
    }

    private fun run() {
        if (down || !isRunning()) {
            Log.d(TAG, "reconnect skipped (down=$down)")
            return
        }
        Log.i(TAG, "network switched, reconnecting")
        // The new network is not always usable the moment it is reported, so the
        // peer retry is repeated: the first ticks cover a fast handover, the last
        // ones a slow cellular attach.
        for ((index, delay) in RETRY_DELAYS_MS.withIndex()) {
            attempts.add(
                executor.schedule({
                    if (!down && isRunning()) {
                        try {
                            reconnect(index)
                        } catch (t: Throwable) {
                            Log.w(TAG, "reconnect failed", t)
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS),
            )
        }
    }
}
