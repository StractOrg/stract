<script lang="ts">
	import type { Experiment } from '$lib';
	import DeleteIcon from '~icons/heroicons/trash';
	import RenameIcon from '~icons/heroicons/pencil-square';
	import { createEventDispatcher } from 'svelte';

	const deleteExperiment = async (id: number) => {
		await fetch('/api/experiments/delete', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ id })
		});

		dispatch('delete', { id });
	};

	const renameExperiment = (id: number) => {
		editable = true;
	};

	const saveRename = async () => {
		editable = false;

		await fetch('/api/experiments/rename', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ id: experiment.id, name: paragraph?.innerText })
		});
	};

	export let experiment: Experiment;
	export let selectionCallback: (id: number) => boolean;
	export let deselectionCallback: (id: number) => void;

	let selected: boolean = false;

	let editable = false;

	let paragraph: HTMLParagraphElement | null = null;

	const dispatch = createEventDispatcher();

	$: {
		if (paragraph) {
			paragraph.addEventListener('blur', async () => {
				await saveRename();
			});

			paragraph.addEventListener('keydown', async (e) => {
				if (e.key === 'Enter') {
					e.preventDefault();
					await saveRename();
				}
			});
		}

		if (editable) {
			setTimeout(() => {
				paragraph?.focus();
			}, 10);
		} else {
			paragraph?.blur();
		}
	}

	export const onClick = () => {
		if (editable) {
			return;
		}

		if (!selected) {
			selected = selectionCallback(experiment.id);
		} else {
			deselectionCallback(experiment.id);
			selected = false;
		}
	};
</script>

<button class="flex items-center gap-x-2 {selected ? 'bg-sky-100' : ''}" on:click={onClick}>
	<button class="text-gray-400 hover:text-red-500" on:click={() => deleteExperiment(experiment.id)}>
		<DeleteIcon class="h-4 w-4" />
	</button>
	<button
		class="text-gray-400 hover:text-blue-500"
		on:click={() => renameExperiment(experiment.id)}
	>
		<RenameIcon class="h-4 w-4" />
	</button>
	<p bind:this={paragraph} class="w-full text-center" contenteditable={editable}>
		{experiment.name}
	</p>
</button>
