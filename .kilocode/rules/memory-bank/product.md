# Product Description: Realize File Syncer

## Why Realize Exists

Realize is a non-centralized file synchronization utility designed to address the need for flexible and partial file sharing between multiple peers (computers or servers). Unlike traditional cloud sync services that often require all files to be stored centrally and fully on each device, Realize allows peers to selectively store and cache files. This is particularly useful in scenarios where storage is limited, network bandwidth is a concern, or users only need access to subsets of a larger dataset.

## Problems It Solves

*   **Partial Syncing:** Enables peers to sync only necessary portions of a dataset, rather than the entire repository. Some peers might store complete local copies ("Real" files), while others might only cache frequently accessed or metadata-only versions ("Unreal" files).
*   **Decentralization:** Avoids reliance on a central server for file storage and synchronization, facilitating direct peer-to-peer exchange.
*   **Efficient Data Transfer:** Aims to minimize data transfer by using rsync-like delta encoding for updates to existing files.
*   **Cross-Platform Access (Goal):** While initial detailed designs focus on Linux capabilities (OverlayFS), the long-term vision includes providing a custom filesystem experience across different operating systems (Linux, macOS, Windows) via NFSv3 for the "Unreal" cache and eventually read-write capabilities for "Unreal".

## How It Should Work

Realize operates through a system of interconnected peers. Each peer runs a Realize service (daemon) that manages local storage and communicates with other peers.

*   **Arenas:** Files are organized into "Arenas," which are distinct sets of data that can be shared and synced.
*   **Real vs. Unreal:**
    *   **Real Files:** Files that are fully stored and available locally on a peer's filesystem.
    *   **Unreal Files:** Files whose content is not fully stored locally but is known (metadata available) and can be fetched from other peers or served from a local cache.
*   **Filesystem Representation:** Each peer presents a unified view of files, combining its local "Real" files with the "Unreal" files accessible from the network, often through a custom filesystem interface (e.g., FUSE on Linux, or an NFSv3 server).
*   **Synchronization:**
    *   Peers exchange information about their file versions and local changes.
    *   Changes are propagated using an RPC (Remote Procedure Call) mechanism.
    *   The system is designed to handle partial transfers and resume interrupted operations.
    *   Conflict resolution strategies are considered, starting with a non-interfering approach and potentially moving to rules like "most recent wins."
*   **Security:** Communication between peers is secured using TLS with raw public key verification (ED25519), ensuring only authorized peers can connect.

## User Experience Goals

*   **Seamless Access:** Users should be able to access and interact with both local and remote files as if they were part of a single, coherent filesystem.
*   **Control over Storage:** Users/administrators should have control over which files or arenas are stored "Real" (locally) versus "Unreal" (cached/remote-only).
*   **Reliable Syncing:** File changes should be reliably propagated to other interested peers.
*   **Resilience:** The system should be resilient to network interruptions, allowing sync operations to resume.
*   **Transparency (for conflicts):** When conflicts occur, users should be informed and provided with ways to access different versions of a file.
*   **Performance:** File access and synchronization should be reasonably performant, with optimizations like delta syncing for large files.