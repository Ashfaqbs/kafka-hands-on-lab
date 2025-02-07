
---
### 🔹 **JMeter Config for 600 TPS**
To achieve **600 Transactions Per Second (TPS)**, you need to configure JMeter properly.

#### **Key Parameters to Adjust:**
1. **Number of Threads (Users):**  
   - This represents concurrent users sending requests.  
   - To achieve **600 TPS**, a good starting point is **600 users**.  
   
2. **Ramp-up Period (Seconds):**  
   - This defines how quickly JMeter will spin up the users.  
   - If we set **Ramp-up = 10 seconds**, it means JMeter will add **600/10 = 60 users per second**.

3. **Loop Count:**  
   - Set this to **Infinite** or a high value so the test runs continuously for a set duration.

4. **Duration (Seconds):**  
   - If you want to test for 1 minute, set it to **60 seconds**.  
   - This ensures requests are evenly spread.

---
### **📌 Recommended JMeter Config**
| Parameter            | Value |
|----------------------|-------|
| Number of Threads (Users) | 600   |
| Ramp-up Period (Seconds) | 10    |
| Loop Count          | Infinite (or a high value) |
| Duration (Seconds)  | 60    |

---
### **🔹 Steps to Configure JMeter**
1. **Open JMeter** and create a **Thread Group**.  
2. Set **Number of Threads (Users) = 600**.  
3. Set **Ramp-up Period = 10 seconds**.  
4. Check **Specify Thread Lifetime** and set **Duration = 60 seconds**.  
5. Add an **HTTP Request Sampler** to send requests to your Kafka producer API.  
6. Add a **Summary Report** and **View Results Tree** to monitor performance.

---

### **📌 JMeter Config for 1000 TPS**  
| Parameter            | Value |
|----------------------|-------|
| Number of Threads (Users) | 1000  |
| Ramp-up Period (Seconds) | 10    |
| Loop Count          | Infinite (or a high value) |
| Duration (Seconds)  | 60    |

🔹 This will generate **1000 Transactions Per Second (TPS)**.  
🔹 JMeter will spawn **100 users per second** during the **10-second ramp-up period**.  
