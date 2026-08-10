use super::*;

impl super::Gateway {
    /// Fan out one request across a [`PanelConfig`](crate::types::config::PanelConfig)'s
    /// slots (each an existing chain), concurrently, returning every slot's result.
    ///
    /// The panel is **formed** first — each slot's primary model + family is
    /// resolved and the `distinct_by` policy enforced — so a distinctness
    /// violation (or unknown chain/model) fails fast with
    /// [`GatewayError::InvalidConfig`] before any model is called. Each slot then
    /// runs its own [`execute`](Self::execute) scoped to the slot's chain, so it
    /// keeps its fallback legs; one slot failing yields `Err` for **that slot
    /// only** and never sinks the panel. Non-streaming (a panel is aggregate).
    pub async fn execute_panel(
        &self,
        request: &InferenceRequest,
        panel: &crate::types::config::PanelConfig,
    ) -> Result<crate::panel::PanelResponse, GatewayError> {
        if request.capability != panel.capability {
            return Err(GatewayError::InvalidConfig(format!(
                "panel '{}' is {:?} but the request is {:?}",
                panel.id, panel.capability, request.capability
            )));
        }

        let config = self.config.read().await.clone();
        let formed = crate::panel::form_panel(&config, panel)?;

        // Fan out: one execute() per slot, scoped to the slot's chain.
        let calls = formed.slots.iter().map(|slot| {
            let mut req = request.clone();
            req.chain = Some(slot.chain.clone());
            req.model = None;
            req.router = None;
            // Layer this slot's persona (gh#18) onto its request only.
            crate::panel::apply_slot_system_prompt(&mut req, slot.system_prompt.as_deref());
            async move { self.execute(&req).await }
        });
        let results = futures::future::join_all(calls).await;

        // Assemble per-slot results; resolve the family that actually answered
        // and flag runtime family collisions between successful slots.
        let mut slots = Vec::with_capacity(formed.slots.len());
        let mut total_cost = Cost::zero();
        let mut used_families: HashMap<String, String> = HashMap::new();
        let mut collisions = Vec::new();

        for (slot, result) in formed.slots.iter().zip(results) {
            let family = match &result {
                Ok(resp) => resp
                    .model
                    .as_ref()
                    .and_then(|m| config.models.get(m))
                    .and_then(|m| m.family.clone())
                    .or_else(|| Some(slot.family.clone())),
                Err(_) => Some(slot.family.clone()),
            };

            if let Ok(resp) = &result
                && let Some(c) = &resp.actual_cost
            {
                total_cost.input_tokens += c.input_tokens;
                total_cost.output_tokens += c.output_tokens;
                total_cost.total_tokens += c.total_tokens;
                total_cost.input_cost += c.input_cost;
                total_cost.output_cost += c.output_cost;
                total_cost.total_cost += c.total_cost;
            }

            let label = slot.label.clone().unwrap_or_else(|| slot.chain.clone());

            // Runtime distinctness (gh#21): a successful slot whose family a
            // prior slot already produced collides. Non-strict records it and
            // keeps both; strict drops this slot (result → error) so no two
            // returned slots share a family. `distinct_by: None` is exempt.
            let mut result = result;
            if result.is_ok()
                && let Some(fam) = family.clone()
            {
                if let Some(prev) = used_families.get(&fam).cloned() {
                    if panel.strict && panel.distinct_by != crate::types::config::DistinctBy::None {
                        collisions.push(format!(
                            "slot '{label}' dropped under strict distinctness: family '{fam}' already answered by '{prev}'"
                        ));
                        result = Err(GatewayError::InvalidConfig(format!(
                            "dropped under strict distinctness: family '{fam}' already answered by slot '{prev}'"
                        )));
                    } else {
                        collisions.push(format!(
                            "slots '{prev}' and '{label}' both answered with family '{fam}'"
                        ));
                    }
                } else {
                    used_families.insert(fam, label);
                }
            }

            slots.push(crate::panel::PanelSlotResult {
                label: slot.label.clone(),
                chain: slot.chain.clone(),
                family,
                result,
            });
        }

        Ok(crate::panel::PanelResponse {
            slots,
            total_cost,
            collisions,
        })
    }

    /// Resolve a named panel from [`GatewayConfig::panels`](crate::types::config::GatewayConfig::panels)
    /// by [`request.panel`](InferenceRequest::panel) and fan it out via
    /// [`execute_panel`](Self::execute_panel) (gh#19).
    ///
    /// `request.panel` must be set; an unset field or an id absent from the
    /// config is [`GatewayError::InvalidConfig`] (fail fast, before any
    /// inference) — consistent with how unknown chains are rejected.
    pub async fn execute_panel_addressed(
        &self,
        request: &InferenceRequest,
    ) -> Result<crate::panel::PanelResponse, GatewayError> {
        let id = request
            .panel
            .as_deref()
            .ok_or_else(|| GatewayError::InvalidConfig("request.panel is not set".to_string()))?;
        // Clone the stored config out under a short-lived read lock; execute_panel
        // re-acquires it, so the guard must not be held across the call.
        let panel = {
            let config = self.config.read().await;
            config
                .panels
                .get(id)
                .cloned()
                .ok_or_else(|| GatewayError::InvalidConfig(format!("unknown panel '{id}'")))?
        };
        self.execute_panel(request, &panel).await
    }
}
