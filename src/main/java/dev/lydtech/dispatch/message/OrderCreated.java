package dev.lydtech.dispatch.message;

import lombok.*;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderCreated {
    UUID orderId;
    String item;
}
