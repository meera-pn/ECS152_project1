import socket
import time
import struct
import sys

class SlidingWindowSender:
    def __init__(self, receiver_ip, receiver_port, file_path, packet_size=1020):
        """
        Initialize the Stop-and-Wait sender
        """
        self.receiver_ip = receiver_ip
        self.receiver_port = receiver_port
        self.file_path = file_path
        self.packet_size = packet_size
        self.timeout = 0.5  # You may need to tune this
        self.sender_port = 0  # Auto-assigned, just needs to be != 5001
        
        # Metrics tracking
        self.total_packets = 0
        self.packet_delays = []
        
    def create_packet(self, seq_num, data):
        """
        Create a packet with sequence number and data
        
        Packet format: [seq_num (4 bytes)] [data]
        
        Returns:
            bytes: Formatted packet
        """
        # pack sequence number as unsigned int (4 bytes) using struct.pack
        packed_seq_number = int.to_bytes(seq_num, 4, signed=True, byteorder='big')
        # concatenate header with data and return
        #print(f"Creating packet: seq_num={seq_num}, packed bytes={packed_seq_number.hex()}")
        return packed_seq_number + data
    
    def parse_ack(self, ack_packet):
        """
        Parse ACK packet to extract sequence number
        
        Returns:
            int: ACK sequence number
        """
        # unpack the first 4 bytes to get sequence number
        return int.from_bytes(ack_packet[:4], signed=True, byteorder='big')
    
    def send_file(self):
        """
        Main function to send file using stop-and-wait protocol
        
        Returns:
            tuple: (throughput, average_delay, performance_metric)
        """
        # create UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Replace with socket creation
        
        # set socket timeout
        sock.settimeout(self.timeout)
        # bind socket to sender_port
        sock.bind(('', self.sender_port))
        # start timer for throughput calculation
        start_time = time.time()
        try:
            file_data = None
            # read the entire file into memory
            with open(self.file_path, 'rb') as f:
                file_data = f.read()
            
            total_bytes = len(file_data)
            seq_num = 0
            offset = 0
            
            # Caclulation of total number of packets:
            all_packets = (total_bytes + self.packet_size - 1) // self.packet_size
            # Sliding window iniital configurations:
            window = 100
            base = 0  
            next_seq = 0  
            acked_packs = set()  
            send_times = {}  
            packet_data = {}  
            
            # Sliding Window Preparation:
            for packet in range(all_packets):
                offset = packet * self.packet_size
                chunkData = file_data[offset:offset + self.packet_size]
                packet_data[packet] = (offset, chunkData)

            # Continuing loop below until all packets have been acknowledged:
            while len(acked_packs) < all_packets:
                while next_seq < base + window and next_seq < all_packets: # Continuing to send packets here so long as we're in window
                    offset, chunkData = packet_data[next_seq]
                    packet = self.create_packet(offset, chunkData)
                    
                    if next_seq not in send_times: # Update send time
                        send_times[next_seq] = time.time()
                    
                    sock.sendto(packet, (self.receiver_ip, self.receiver_port))
                    next_seq += 1
                

                # Recieving ACKS here:
                try:
                    ack_packet, _ = sock.recvfrom(1024)
                    ack_offset = self.parse_ack(ack_packet)
                    if ack_offset > 0:
                        acked_pkt1 = (ack_offset - 1) // self.packet_size # Converting ACK to a packet number..
                        acked_pkt1 = min(acked_pkt1, all_packets - 1)

                        for packet_number in range(acked_pkt1 + 1):  # This packet, and the ones before it, are now acknowledged
                            if packet_number not in acked_packs:
                                acked_packs.add(packet_number)
                            
                                # Recording delay for this packet (debug)
                                if packet_number in send_times:
                                    delay = time.time() - send_times[packet_number]
                                    self.packet_delays.append(delay)
                    
                        while base in acked_packs and base < all_packets:
                            base += 1
                        
                except socket.timeout:
                    # Handling packet loss (attempt)
                    for packet_number in range(base, min(next_seq, all_packets)):
                        if packet_number not in acked_packs:
                            offset, chunk = packet_data[packet_number]
                            packet = self.create_packet(offset, chunk)
                            sock.sendto(packet, (self.receiver_ip, self.receiver_port))
            
            # Update total packets counter
            self.total_packets = all_packets
            


            # Step 4: Send empty message with correct sequence id to signal end
            # TODO: Create FIN packet (empty data, current seq_num)
            fin_packet = self.create_packet(total_bytes, b'')
            #print(f"Sending FIN packet with seq_num={total_bytes}")

            fin_ack_received = False
            
            while not fin_ack_received:
                try:
                    # TODO: Send FIN packet
                    sock.sendto(fin_packet, (self.receiver_ip, self.receiver_port))
                    # TODO: Wait for ACK and FIN message from receiver
                    ack_packet, _ = sock.recvfrom(1024)
                    
                    # TODO: Parse ACK
                    ack_seq_num = self.parse_ack(ack_packet)
                    #print(f"FIN: Received ACK {ack_seq_num}, expected {total_bytes}")
                    
                    # TODO: Check if ACK matches sequence number
                    if ack_seq_num == total_bytes:
                        fin_ack_received = True
                        
                except socket.timeout:
                    # TODO: Retransmit FIN packet on timeout
                    # print(f"FIN timeout, retrying...")
                    pass
            
            # Step 6: Send FINACK message to let receiver know to exit
            # TODO: Create FINACK message with seq_num+1 and body '==FINACK'
            finack_message = self.create_packet(total_bytes+1, b'==FINACK==')
            #print(f"Sending FINACK with seq_num={total_bytes+1}")
            
            # TODO: Send FINACK message
            #sock.sendto(finack_message, (self.receiver_ip, self.receiver_port))
            #print("FINACK sent!")
            for i in range(3):  # Send 3 times
                sock.sendto(finack_message, (self.receiver_ip, self.receiver_port))
                #print(f"FINACK sent (attempt {i+1})")
                # Give receiver time to process FINACK
                time.sleep(0.1)
            
        finally:
            # TODO: Close socket
            sock.close()
            pass
        
        # TODO: Stop timer
        end_time = time.time()
        total_time = end_time - start_time
        

        # -------CALCULATING METRICS---------
        # TODO: Calculate throughput (total_bytes / total_time)
        throughput = total_bytes/total_time
        
        # TODO: Calculate average delay (sum of packet_delays / number of packets)
        average_delay = sum(self.packet_delays)/len(self.packet_delays)
        
        # TODO: Calculate performance metric using the given formula
        # Metric = 0.3 * (Throughput/1000) + 0.7 / Average_Delay
        performance_metric = 0.3 * (throughput / 1000) + 0.7 / average_delay
        
        return throughput, average_delay, performance_metric


def run_multiple_trials(receiver_ip, receiver_port, file_path, num_trials=1):
    """
    Run the sender multiple times and compute average metrics
    """
    throughputs = []
    delays = []
    metrics = []
    
    for trial in range(num_trials):
        # TODO: Create sender instance
        sender = SlidingWindowSender(receiver_ip, receiver_port, file_path)
        
        # TODO: Call send_file() and get metrics
        throughput, avg_delay, metric = sender.send_file()
        
        # TODO: Append metrics to respective lists
        throughputs.append(throughput)
        delays.append(avg_delay)
        metrics.append(metric)
        # Add delay between trials to ensure receiver is ready
        time.sleep(5) 
    
    # TODO: Calculate average throughput
    avg_throughput = sum(throughputs) / len(throughputs)
    
    # TODO: Calculate average delay
    avg_delay = sum(delays) / len(delays)
    
    # TODO: Calculate average metric
    avg_metric = sum(metrics) / len(metrics)
    
    # TODO: Print output in required format (3 values, 7 decimal points, comma-separated)
    # Format: "throughput,delay,metric" with .7f formatting
    print(f"{avg_throughput:.7f},{avg_delay:.7f},{avg_metric:.7f}")



if __name__ == "__main__":
    # TODO: Parse command line arguments
    # Expected: python stop_and_wait.py <file_path>
    
    if len(sys.argv) != 2:
        print("Usage: python stop_and_wait.py <file_path>")
        sys.exit(1)
    
    receiver_ip = "localhost"
    receiver_port = 5001
    
    # TODO: Get file_path from command line arguments
    file_path = sys.argv[1]
 

    run_multiple_trials(receiver_ip, receiver_port, file_path)
