# Phase 1: AWS Networking, Security & Compute
### *Engineering Deep-Dive & Senior Technical Interview Mastery Guide*

---

## 1. Architectural Decisions: The "Why" Behind the Code

### Decision 1: Why a Custom VPC instead of the Default VPC?
* **The Naive Approach:** Launching an EC2 instance into AWS's Default VPC (`172.31.0.0/16`).
* **The Production Reality:**
  1. **Network Isolation:** The Default VPC has public subnets in every Availability Zone with default internet routing. Any instance launched there automatically receives a public IP address unless disabled—violating enterprise Zero Trust security principles.
  2. **CIDR Overlap:** Most enterprise on-premises networks or partner VPCs use standard IP ranges. If you ever need to set up AWS Direct Connect, VPN, or VPC Peering to Databricks/Snowflake, overlapping CIDR blocks make peering impossible without complex Private NAT.
  3. **Blast Radius Control:** A custom VPC (`10.0.0.0/16`) creates an explicit boundary. We control routing tables, subnet tiering (public vs. private), and internet gateways.

---

### Decision 2: CIDR Math & Subnetting Strategy
* **VPC CIDR: `10.0.0.0/16`**
  - Prefix `/16` leaves $32 - 16 = 16$ host bits.
  - Total IP addresses = $2^{16} = 65,536$ addresses.
* **Public Subnet CIDR: `10.0.1.0/24`**
  - Prefix `/24` leaves $32 - 24 = 8$ host bits.
  - Total addresses = $2^8 = 256$ addresses.
  - **AWS Reserved IPs:** In *every* AWS subnet, 5 IP addresses are reserved:
    1. `10.0.1.0`: Network address.
    2. `10.0.1.1`: VPC Router address.
    3. `10.0.1.2`: Amazon-provided DNS (Route 53 Resolver).
    4. `10.0.1.3`: Reserved by AWS for future use.
    5. `10.0.1.255`: Network broadcast address (AWS does not support broadcast, but reserves it).
  - *Usable IPs:* $256 - 5 = 251$ usable IP addresses.

---

### Decision 3: Security Groups vs. NACLs (Stateful vs. Stateless)
* **What We Used:** Security Group `social-media-lakehouse-sg` opening port `22` (SSH) and port `9092` (Kafka External Listener).
* **The Fundamental Difference:**
  | Feature | Security Group (SG) | Network ACL (NACL) |
  | :--- | :--- | :--- |
  | **Scope** | Instance / ENI level | Subnet boundary level |
  | **Statefulness** | **Stateful**: Return traffic is automatically allowed regardless of inbound rules. | **Stateless**: Inbound and outbound traffic must be explicitly allowed. |
  | **Rule Evaluation**| All rules evaluated; default deny. | Processed in strict numerical order (100, 200...); first match wins. |
  | **Deny Rules** | Cannot explicitly define DENY (only ALLOW). | Supports both ALLOW and DENY rules. |

---

### Decision 4: IAM Instance Profiles vs. Static Access Keys
* **The Antipattern:** Running `aws configure` on an EC2 instance and storing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `~/.aws/credentials` or environment variables.
  - *Failure Mode:* Anyone who SSHs into the box, or any compromised container process with read access, steals permanent cloud credentials.
* **The Enterprise Standard:** **IAM Instance Profiles & Roles**.
  - The EC2 instance assumes an IAM Role via the AWS Security Token Service (STS).
  - Credentials are provided automatically via the Instance Metadata Service (IMDSv2 at `169.254.169.254`).
  - Keys are **short-lived, rotated automatically every few hours**, and never stored on disk.

---

### Decision 5: Why Swap Space on EC2?
* Running Kubernetes (Minikube) + Docker + Apache Kafka + Zookeeper/Kraft requires ~2.5GB to 3.5GB of RAM.
* On affordable AWS instances (such as `t3.small` or `t3.medium`), memory pressure triggers the Linux kernel **OOM (Out Of Memory) Killer**, terminating the Kafka Java process or the Minikube API server.
* Configuring a **2GB swap partition** provides a virtual memory buffer to absorb spikes, guaranteeing zero pod evictions or JVM crashes during streaming load.

---

## 2. Senior Interview Questions & Model Answers

### Q1: "How did you design the networking for your streaming data platform on AWS?"
> **Model Answer:**  
> *"Instead of using the default VPC, I architected a custom VPC with a `/16` CIDR block (`10.0.0.0/16`) to avoid IP overlap and enforce strict network segmentation. I established a public subnet associated with an Internet Gateway for compute and broker ingress, paired with a custom route table. For security, I separated concerns: compute resources are guarded by stateful Security Groups that restrict inbound traffic exclusively to port 22 for secure SSH access and port 9092 for Kafka external stream consumption. Furthermore, instead of provisioning static IAM credentials on the host, I attached an IAM instance profile using STS temporary credentials to adhere to the principle of least privilege for downstream S3 access."*

---

### Q2: "In production, would Kafka brokers sit in a public subnet? How would you architect this in a multi-region or hybrid enterprise setup?"
> **Model Answer:**  
> *"In a production enterprise environment, Kafka brokers would strictly reside in **private subnets** across multiple Availability Zones to ensure high availability and zero public internet exposure.  
> External or cross-cloud consumers (such as Databricks running in its own VPC) would access Kafka through one of three enterprise patterns:
> 1. **VPC Peering or AWS Transit Gateway:** If Databricks is in AWS, routing traffic over AWS private backbone network.
> 2. **AWS PrivateLink (VPC Endpoint Services):** Exposing the Kafka brokers behind an internal Network Load Balancer (NLB) and presenting them as private endpoints directly in the consumer's VPC without exposing the cluster to the public internet.
> 3. **Mutual TLS (mTLS) with SASL/SCRAM:** If internet transit is unavoidable, wrapping the external broker listener in TLS encryption with cryptographic client certificate validation and SASL authentication."*

---

### Q3: "What is the AWS Instance Metadata Service (IMDS), and why is IMDSv2 important for data infrastructure?"
> **Model Answer:**  
> *"IMDS operates at the link-local IP address `169.254.169.254`. When an IAM Role is attached to an EC2 instance, the AWS SDK fetches temporary credentials from IMDS.  
> Under IMDSv1, a simple HTTP GET request could retrieve credentials, creating a vulnerability against Server-Side Request Forgery (SSRF). For instance, if an open-source web UI or an unvetted container runs on the node, an SSRF attack could exfiltrate the role credentials.  
> IMDSv2 mitigates this by requiring a session-oriented model: you must first initiate a `PUT` request to obtain a cryptographic session token with an enforced TTL (hop limit), and then supply that token in the header of subsequent GET requests. This neutralizes reverse-proxy and SSRF exfiltration."*

---

### Q4: "Your Kafka pod crashes randomly on EC2 with exit code 137. How do you diagnose and fix it?"
> **Model Answer:**  
> *"Exit code 137 represents a `SIGKILL` ($128 + 9$), which almost always indicates the Linux kernel **Out-of-Memory (OOM) Killer** terminated the process.  
> To diagnose:
> 1. Run `dmesg -T | grep -i oom` on the host to verify if `java` (Kafka) was killed by the OS.
> 2. Check `kubectl describe pod <kafka-pod>` to inspect termination reasons (`OOMKilled: true`).
> To fix:
> 1. Adjust the JVM heap options: Kafka defaults to 1G/1G heap (`KAFKA_HEAP_OPTS="-Xms512M -Xmx512M"` for resource-constrained instances).
> 2. Enforce Kubernetes resource requests and limits in the deployment manifest.
> 3. Provision a dedicated Linux swap partition (`dd if=/dev/zero of=/swapfile`) to provide headroom for temporary memory spikes."*
