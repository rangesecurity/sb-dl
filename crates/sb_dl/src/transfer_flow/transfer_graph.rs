use {
    super::types::TransferFlow,
    crate::transfer_flow::types::Transfer,
    anyhow::{anyhow, Context, Result},
    petgraph::{dot::Dot, graph::DiGraph},
};

pub fn prepare_transfer_graph(transfer_flow: TransferFlow) -> Result<()> {
    let mut ordered_transfers: Vec<Transfer> = vec![];
    let mut keys = transfer_flow.keys().map(|key| *key).collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let (outer_transfer, inner_transfers) = transfer_flow
            .get(&key)
            .with_context(|| "should not be None")?;
        if let Some(transfer) = outer_transfer {
            let transfer: Transfer = From::from(transfer.clone());
            ordered_transfers.push(transfer);
        }
        if !inner_transfers.contains_key(&key) {
            // no inner transfers
            continue;
        }
        let inner_transfers = inner_transfers
            .get(&key)
            .with_context(|| format!("should not be None for key {key}"))?;
        for inner_transfer in inner_transfers {
            let transfer: Transfer = From::from(inner_transfer.clone());
            ordered_transfers.push(transfer);
        }
    }
    let mut graph = DiGraph::new();

    // Map to store node indices
    let mut node_indices = std::collections::HashMap::new();
    for transfer in &ordered_transfers {
        let sender_idx = *node_indices
            .entry(transfer.sender.clone())
            .or_insert_with(|| graph.add_node(transfer.sender.clone()));

        let receiver_idx = *node_indices
            .entry(transfer.recipient.clone())
            .or_insert_with(|| graph.add_node(transfer.recipient.clone()));

        graph.add_edge(
            sender_idx,
            receiver_idx,
            (transfer.mint.clone(), transfer.amount.clone()),
        );
    }
    // Generate the dot format
    let dot = Dot::with_config(&graph, &[]);

    println!("{:?}", dot);
    Ok(())
}
