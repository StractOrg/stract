// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use kuchiki::NodeRef;
use serde::{Deserialize, Serialize};

mod json_ld;
mod microdata;

type Text = String;
type Wrapper<T> = Option<OneOrMany<Box<T>>>;

pub fn parse(root: NodeRef) -> Vec<SchemaOrg> {
    let mut res = self::json_ld::parse(root.clone());
    res.append(&mut self::microdata::parse_schema(root));

    res
}

#[non_exhaustive]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(tag = "@type")]
pub enum SchemaOrg {
    Thing(Thing),
    Intangible(Intangible),
    Organization(Organization),
    Person(Person),
    ImageObject(ImageObject),
    PostalAddress(PostalAddress),
    Country(Country),
    Place(Place),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub fn one(self) -> Option<T> {
        match self {
            OneOrMany::One(one) => Some(one),
            OneOrMany::Many(many) => many.into_iter().next(),
        }
    }

    pub fn many(self) -> Vec<T> {
        match self {
            OneOrMany::One(one) => vec![one],
            OneOrMany::Many(many) => many,
        }
    }
}

impl From<String> for OneOrMany<String> {
    fn from(value: String) -> Self {
        Self::One(value)
    }
}

/// https://schema.org/Thing
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Thing {
    pub name: Wrapper<Text>,
    pub description: Wrapper<Text>,
    pub disambiguating_description: Wrapper<Text>,
    pub alternate_name: Wrapper<Text>,
    pub additional_type: Wrapper<Text>,
    pub image: Wrapper<Text>,
    pub url: Wrapper<Text>,
}

/// https://schema.org/Action
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Action {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/AchieveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AchieveAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/LoseAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LoseAction {
    #[serde(flatten)]
    pub achieve_action: AchieveAction,
}

/// https://schema.org/TieAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TieAction {
    #[serde(flatten)]
    pub achieve_action: AchieveAction,
}

/// https://schema.org/WinAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WinAction {
    #[serde(flatten)]
    pub achieve_action: AchieveAction,
}

/// https://schema.org/AssessAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AssessAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/ChooseAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChooseAction {
    #[serde(flatten)]
    pub assess_action: AssessAction,
}

/// https://schema.org/IgnoreAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IgnoreAction {
    #[serde(flatten)]
    pub assess_action: AssessAction,
}

/// https://schema.org/ReactAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReactAction {
    #[serde(flatten)]
    pub assess_action: AssessAction,
}

/// https://schema.org/ReviewAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReviewAction {
    #[serde(flatten)]
    pub assess_action: AssessAction,
}

/// https://schema.org/ConsumeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/DrinkAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DrinkAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/EatAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EatAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/InstallAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InstallAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/ListenAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListenAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/PlayGameAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlayGameAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/ReadAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReadAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/UseAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UseAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/ViewAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ViewAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/WatchAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WatchAction {
    #[serde(flatten)]
    pub consume_action: ConsumeAction,
}

/// https://schema.org/ControlAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ControlAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/ActivateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ActivateAction {
    #[serde(flatten)]
    pub control_action: ControlAction,
}

/// https://schema.org/DeactivateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeactivateAction {
    #[serde(flatten)]
    pub control_action: ControlAction,
}

/// https://schema.org/ResumeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResumeAction {
    #[serde(flatten)]
    pub control_action: ControlAction,
}

/// https://schema.org/SuspendAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SuspendAction {
    #[serde(flatten)]
    pub control_action: ControlAction,
}

/// https://schema.org/CreateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/CookAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CookAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/DrawAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DrawAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/FilmAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FilmAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/PaintAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaintAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/PhotographAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PhotographAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/WriteAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WriteAction {
    #[serde(flatten)]
    pub create_action: CreateAction,
}

/// https://schema.org/FindAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FindAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/CheckAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CheckAction {
    #[serde(flatten)]
    pub find_action: FindAction,
}

/// https://schema.org/DiscoverAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverAction {
    #[serde(flatten)]
    pub find_action: FindAction,
}

/// https://schema.org/TrackAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrackAction {
    #[serde(flatten)]
    pub find_action: FindAction,
}

/// https://schema.org/InteractAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InteractAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/BefriendAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BefriendAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/CommunicateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommunicateAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/FollowAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FollowAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/JoinAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JoinAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/LeaveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LeaveAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/MarryAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarryAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/RegisterAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RegisterAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/SubscribeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/UnRegisterAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UnRegisterAction {
    #[serde(flatten)]
    pub interact_action: InteractAction,
}

/// https://schema.org/MoveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MoveAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/ArriveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ArriveAction {
    #[serde(flatten)]
    pub move_action: MoveAction,
}

/// https://schema.org/DepartAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DepartAction {
    #[serde(flatten)]
    pub move_action: MoveAction,
}

/// https://schema.org/TravelAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TravelAction {
    #[serde(flatten)]
    pub move_action: MoveAction,
}

/// https://schema.org/OrganizeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrganizeAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/AllocateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AllocateAction {
    #[serde(flatten)]
    pub organize_action: OrganizeAction,
}

/// https://schema.org/ApplyAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ApplyAction {
    #[serde(flatten)]
    pub organize_action: OrganizeAction,
}

/// https://schema.org/BookmarkAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookmarkAction {
    #[serde(flatten)]
    pub organize_action: OrganizeAction,
}

/// https://schema.org/PlanAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlanAction {
    #[serde(flatten)]
    pub organize_action: OrganizeAction,
}

/// https://schema.org/PlayAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlayAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/ExerciseAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExerciseAction {
    #[serde(flatten)]
    pub play_action: PlayAction,
}

/// https://schema.org/PerformAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PerformAction {
    #[serde(flatten)]
    pub play_action: PlayAction,
}

/// https://schema.org/SearchAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SearchAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/SeekToAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SeekToAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/SolveMathAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolveMathAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/TradeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/BuyAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BuyAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/DonateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DonateAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/OrderAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/PayAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PayAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/PreOrderAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PreOrderAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/QuoteAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QuoteAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/RentAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RentAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/SpellAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpellAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/TipAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TipAction {
    #[serde(flatten)]
    pub trade_action: TradeAction,
}

/// https://schema.org/TransferAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TransferAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/BorrowAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BorrowAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/DownloadAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DownloadAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/GiveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GiveAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/LendAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LendAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/MoneyTransfer
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MoneyTransfer {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/ReceiveAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReceiveAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/ReturnAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReturnAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/SendAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SendAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/TakeAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TakeAction {
    #[serde(flatten)]
    pub transfer_action: TransferAction,
}

/// https://schema.org/UpdateAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAction {
    #[serde(flatten)]
    pub action: Action,
}

/// https://schema.org/AddAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddAction {
    #[serde(flatten)]
    pub update_action: UpdateAction,
}

/// https://schema.org/DeleteAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAction {
    #[serde(flatten)]
    pub update_action: UpdateAction,
}

/// https://schema.org/ReplaceAction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReplaceAction {
    #[serde(flatten)]
    pub update_action: UpdateAction,
}

/// https://schema.org/BioChemEntity
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BioChemEntity {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/ChemicalSubstance
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChemicalSubstance {
    #[serde(flatten)]
    pub bio_chem_entity: BioChemEntity,
}
/// https://schema.org/Gene
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Gene {
    #[serde(flatten)]
    pub bio_chem_entity: BioChemEntity,
}

/// https://schema.org/MolecularEntity
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MolecularEntity {
    #[serde(flatten)]
    pub bio_chem_entity: BioChemEntity,
}

/// https://schema.org/Protein
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Protein {
    #[serde(flatten)]
    pub bio_chem_entity: BioChemEntity,
}

/// https://schema.org/CreativeWork
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreativeWork {
    #[serde(flatten)]
    pub thing: Thing,
    pub author: Wrapper<PersonOrOrganization>,
}

/// https://schema.org/AmpStory
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AmpStory {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/ArchiveComponent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveComponent {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Article
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Article {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/AdvertiserContentArticle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AdvertiserContentArticle {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/NewsArticle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewsArticle {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/Report
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/SatiricalArticle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SatiricalArticle {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/ScholarlyArticle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ScholarlyArticle {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/SocialMediaPosting
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SocialMediaPosting {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/TechArticle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TechArticle {
    #[serde(flatten)]
    pub article: Article,
}

/// https://schema.org/Atlas
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Atlas {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Blog
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Blog {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Book
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Book {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Audiobook
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Audiobook {
    #[serde(flatten)]
    pub book: Book,
}

/// https://schema.org/Chapter
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Chapter {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Claim
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Clip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Clip {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MovieClip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MovieClip {
    #[serde(flatten)]
    pub clip: Clip,
}

/// https://schema.org/RadioClip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadioClip {
    #[serde(flatten)]
    pub clip: Clip,
}

/// https://schema.org/TVClip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TVClip {
    #[serde(flatten)]
    pub clip: Clip,
}

/// https://schema.org/VideoGameClip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VideoGameClip {
    #[serde(flatten)]
    pub clip: Clip,
}

/// https://schema.org/Code
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Code {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Collection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Collection {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/ComicStory
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComicStory {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/ComicCoverArt
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComicCoverArt {
    #[serde(flatten)]
    pub comic_story: ComicStory,
}

/// https://schema.org/Comment
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Answer
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Answer {
    #[serde(flatten)]
    pub comment: Comment,
}

/// https://schema.org/CorrectionComment
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CorrectionComment {
    #[serde(flatten)]
    pub comment: Comment,
}

/// https://schema.org/Question
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Question {
    #[serde(flatten)]
    pub comment: Comment,
}

/// https://schema.org/Conversation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Conversation {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Course
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Course {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub learning_resource: LearningResource,
}

/// https://schema.org/CreativeWorkSeason
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreativeWorkSeason {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/PodcastSeason
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PodcastSeason {
    #[serde(flatten)]
    pub creative_work_season: CreativeWorkSeason,
}

/// https://schema.org/RadioSeason
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadioSeason {
    #[serde(flatten)]
    pub creative_work_season: CreativeWorkSeason,
}

/// https://schema.org/CreativeWorkSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreativeWorkSeries {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub series: Series,
}

/// https://schema.org/BookSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/MovieSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MovieSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/Periodical
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Periodical {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/PodcastSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PodcastSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/RadioSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadioSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/TVSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TVSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/VideoGameSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VideoGameSeries {
    #[serde(flatten)]
    pub creative_work_series: CreativeWorkSeries,
}

/// https://schema.org/DataCatalog
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataCatalog {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Dataset
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/DataFeed
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataFeed {
    #[serde(flatten)]
    pub dataset: Dataset,
}

/// https://schema.org/DefinedTermSet
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DefinedTermSet {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/CategoryCodeSet
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CategoryCodeSet {
    #[serde(flatten)]
    pub defined_term_set: DefinedTermSet,
}

/// https://schema.org/Diet
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Diet {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/DigitalDocument
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DigitalDocument {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/NoteDigitalDocument
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NoteDigitalDocument {
    #[serde(flatten)]
    pub digital_document: DigitalDocument,
}

/// https://schema.org/PresentationDigitalDocument
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PresentationDigitalDocument {
    #[serde(flatten)]
    pub digital_document: DigitalDocument,
}

/// https://schema.org/SpreadsheetDigitalDocument
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpreadsheetDigitalDocument {
    #[serde(flatten)]
    pub digital_document: DigitalDocument,
}

/// https://schema.org/TextDigitalDocument
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TextDigitalDocument {
    #[serde(flatten)]
    pub digital_document: DigitalDocument,
}

/// https://schema.org/Drawing
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Drawing {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/EducationalOccupationalCredential
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EducationalOccupationalCredential {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Episode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Episode {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/PodcastEpisode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PodcastEpisode {
    #[serde(flatten)]
    pub episode: Episode,
}

/// https://schema.org/RadioEpisode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadioEpisode {
    #[serde(flatten)]
    pub episode: Episode,
}

/// https://schema.org/TVEpisode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TVEpisode {
    #[serde(flatten)]
    pub episode: Episode,
}

/// https://schema.org/ExercisePlan
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExercisePlan {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Game
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Game {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/VideoGame
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VideoGame {
    #[serde(flatten)]
    pub game: Game,
    #[serde(flatten)]
    pub software_application: SoftwareApplication,
}

/// https://schema.org/Guide
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Guide {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/HowTo
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowTo {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Recipe
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Recipe {
    #[serde(flatten)]
    pub how_to: HowTo,
}

/// https://schema.org/HowToDirection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowToDirection {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub list_item: ListItem,
}

/// https://schema.org/HowToSection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowToSection {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub item_list: ItemList,
    #[serde(flatten)]
    pub list_item: ListItem,
}

/// https://schema.org/HowToStep
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowToStep {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub item_list: ItemList,
    #[serde(flatten)]
    pub list_item: ListItem,
}

/// https://schema.org/HowToTip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowToTip {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub list_item: ListItem,
}

/// https://schema.org/HyperToc
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HyperToc {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/HyperTocEntry
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HyperTocEntry {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/LearningResource
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LearningResource {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Quiz
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Quiz {
    #[serde(flatten)]
    pub learning_resource: LearningResource,
}

/// https://schema.org/Legislation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Legislation {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/LegislationObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LegislationObject {
    #[serde(flatten)]
    pub legislation: Legislation,
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/Manuscript
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Manuscript {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Map
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Map {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MathSolver
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MathSolver {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MediaObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MediaObject {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    pub content_url: Wrapper<Text>,
}

/// https://schema.org/3DModel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase", rename = "3DModel")]
pub struct ThreeDModel {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/AudioObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AudioObject {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/DataDownload
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataDownload {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/ImageObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ImageObject {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/MusicVideoObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicVideoObject {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/VideoObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VideoObject {
    #[serde(flatten)]
    pub media_object: MediaObject,
}

/// https://schema.org/MediaReviewItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MediaReviewItem {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Menu
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Menu {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MenuSection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MenuSection {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Message
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/EmailMessage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EmailMessage {
    #[serde(flatten)]
    pub message: Message,
}

/// https://schema.org/Movie
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Movie {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MusicComposition
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicComposition {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MusicPlaylist
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicPlaylist {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MusicAlbum
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicAlbum {
    #[serde(flatten)]
    pub music_playlist: MusicPlaylist,
}

/// https://schema.org/MusicRelease
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicRelease {
    #[serde(flatten)]
    pub music_playlist: MusicPlaylist,
}

/// https://schema.org/MusicRecording
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicRecording {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Painting
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Painting {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Photograph
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Photograph {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Play
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Play {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Poster
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Poster {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/PublicationIssue
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublicationIssue {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/ComicIssue
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComicIssue {
    #[serde(flatten)]
    pub publication_issue: PublicationIssue,
}

/// https://schema.org/PublicationVolume
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublicationVolume {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Quotation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Quotation {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Review
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Review {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/ClaimReview
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClaimReview {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/CriticReview
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CriticReview {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/EmployerReview
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EmployerReview {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/MediaReview
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MediaReview {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/Recommendation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/UserReview
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserReview {
    #[serde(flatten)]
    pub review: Review,
}

/// https://schema.org/Sculpture
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Sculpture {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Season
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Season {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/SheetMusic
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SheetMusic {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/ShortStory
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShortStory {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/SoftwareApplication
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SoftwareApplication {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/MobileApplication
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MobileApplication {
    #[serde(flatten)]
    pub software_application: SoftwareApplication,
}

/// https://schema.org/WebApplication
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebApplication {
    #[serde(flatten)]
    pub software_application: SoftwareApplication,
}

/// https://schema.org/SoftwareSourceCode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SoftwareSourceCode {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/SpecialAnnouncement
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpecialAnnouncement {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Statement
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/TVSeason
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TVSeason {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
    #[serde(flatten)]
    pub creative_work_season: CreativeWorkSeason,
}

/// https://schema.org/TvSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TvSeries {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Thesis
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Thesis {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/VisualArtwork
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VisualArtwork {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/CoverArt
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CoverArt {
    #[serde(flatten)]
    pub visual_artwork: VisualArtwork,
}

/// https://schema.org/WebContent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebContent {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}
/// https://schema.org/HealthTopicContent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthTopicContent {
    #[serde(flatten)]
    pub web_content: WebContent,
}

/// https://schema.org/WebPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebPage {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/AboutPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AboutPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/CheckoutPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/CollectionPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CollectionPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/ContactPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContactPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/FAQPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FAQPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/ItemPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ItemPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/MedicalWebPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalWebPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/ProfilePage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProfilePage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/QAPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QAPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/RealEstateListing
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RealEstateListing {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/SearchResultsPage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SearchResultsPage {
    #[serde(flatten)]
    pub web_page: WebPage,
}

/// https://schema.org/WebPageElement
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebPageElement {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/SiteNavigationElement
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SiteNavigationElement {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/Table
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/WPAdBlock
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WPAdBlock {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/WPFooter
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WPFooter {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/WPHeader
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WPHeader {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/WPSideBar
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WPSideBar {
    #[serde(flatten)]
    pub web_page_element: WebPageElement,
}

/// https://schema.org/WebSite
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebSite {
    #[serde(flatten)]
    pub creative_work: CreativeWork,
}

/// https://schema.org/Event
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/BusinessEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusinessEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/ChildrensEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChildrensEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/ComedyEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComedyEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/CourseInstance
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CourseInstance {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/DanceEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DanceEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/DeliveryEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/EducationEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EducationEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/EventSeries
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventSeries {
    #[serde(flatten)]
    pub event: Event,
    #[serde(flatten)]
    pub series: Series,
}

/// https://schema.org/ExhibitionEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExhibitionEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/Festival
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Festival {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/FoodEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FoodEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/Hackathon
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Hackathon {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/LiteraryEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LiteraryEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/MusicEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/PublicationEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublicationEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/BroadcastEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastEvent {
    #[serde(flatten)]
    pub publication_event: PublicationEvent,
}

/// https://schema.org/OnDemandEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OnDemandEvent {
    #[serde(flatten)]
    pub publication_event: PublicationEvent,
}

/// https://schema.org/SaleEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SaleEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/ScreeningEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ScreeningEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/SocialEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SocialEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/SportsEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SportsEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/TheaterEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TheaterEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/UserInteraction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserInteraction {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/UserBlocks
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserBlocks {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserCheckins
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserCheckins {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserComments
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserComments {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserDownloads
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserDownloads {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserLikes
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserLikes {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserPageVisits
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserPageVisits {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserPlays
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserPlays {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserPlusOnes
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserPlusOnes {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/UserTweets
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserTweets {
    #[serde(flatten)]
    pub user_interaction: UserInteraction,
}

/// https://schema.org/VisualArtsEvent
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VisualArtsEvent {
    #[serde(flatten)]
    pub event: Event,
}

/// https://schema.org/Intangible
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Intangible {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/ActionAccessSpecification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ActionAccessSpecification {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/AlignmentObject
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlignmentObject {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Audience
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Audience {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/BusinessAudience
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusinessAudience {
    #[serde(flatten)]
    pub audience: Audience,
}

/// https://schema.org/EducationalAudience
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EducationalAudience {
    #[serde(flatten)]
    pub audience: Audience,
}

/// https://schema.org/MedicalAudience
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalAudience {
    #[serde(flatten)]
    pub audience: Audience,
}

/// https://schema.org/PeopleAudience
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PeopleAudience {
    #[serde(flatten)]
    pub audience: Audience,
}

/// https://schema.org/Researcher
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Researcher {
    #[serde(flatten)]
    pub audience: Audience,
}

/// https://schema.org/BedDetails
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BedDetails {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Brand
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Brand {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/BroadcastChannel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastChannel {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/RadioChannel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadioChannel {
    #[serde(flatten)]
    pub broadcast_channel: BroadcastChannel,
}

/// https://schema.org/TelevisionChannel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TelevisionChannel {
    #[serde(flatten)]
    pub broadcast_channel: BroadcastChannel,
}

/// https://schema.org/BroadcastFrequencySpecification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastFrequencySpecification {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Class
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Class {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/ComputerLanguage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ComputerLanguage {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/DataFeedItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataFeedItem {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/DefinedTerm
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DefinedTerm {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/CategoryCode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CategoryCode {
    #[serde(flatten)]
    pub defined_term: DefinedTerm,
}

/// https://schema.org/Demand
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Demand {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/DigitalDocumentPermission
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DigitalDocumentPermission {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/EducationalOccupationalProgram
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EducationalOccupationalProgram {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/WorkBasedProgram
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkBasedProgram {
    #[serde(flatten)]
    pub educational_occupational_program: EducationalOccupationalProgram,
}

/// https://schema.org/EnergyConsumptionDetails
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnergyConsumptionDetails {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/EntryPoint
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EntryPoint {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Enumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enumeration {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/AdultOrientedEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AdultOrientedEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/BoardingPolicyType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BoardingPolicyType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/BookFormatType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookFormatType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/BusinessEntityType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusinessEntityType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/BusinessFunction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusinessFunction {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/CarUsageType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CarUsageType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ContactPointOption
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContactPointOption {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/DayOfWeek
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DayOfWeek {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/DeliveryMethod
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryMethod {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/DigitalDocumentPermissionType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DigitalDocumentPermissionType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/DigitalPlatformEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DigitalPlatformEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/EnergyEfficiencyEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnergyEfficiencyEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/EventAttendanceModeEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventAttendanceModeEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/GameAvailabilityEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GameAvailabilityEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/GamePlayMode
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GamePlayMode {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/GenderType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GenderType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/GovernmentBenefitsType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GovernmentBenefitsType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/HealthAspectEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthAspectEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ItemAvailability
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ItemAvailability {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ItemListOrderType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ItemListOrderType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/LegalValueLevel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LegalValueLevel {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MapCategoryType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MapCategoryType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MeasurementTypeEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MeasurementTypeEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MediaManipulationRatingEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MediaManipulationRatingEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MedicalEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MerchantReturnEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MerchantReturnEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MusicAlbumProductionType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicAlbumProductionType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MusicAlbumReleaseType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicAlbumReleaseType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/MusicReleaseFormatType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MusicReleaseFormatType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/NonprofitType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NonprofitType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/OfferItemCondition
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OfferItemCondition {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/PaymentMethod
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaymentMethod {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/PhysicalActivityCategory
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PhysicalActivityCategory {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/PriceComponentTypeEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PriceComponentTypeEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/PriceTypeEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PriceTypeEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/QualitativeValue
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QualitativeValue {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/RefundTypeEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RefundTypeEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/RestrictedDiet
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RestrictedDiet {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ReturnFeesEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReturnFeesEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ReturnLabelSourceEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReturnLabelSourceEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/ReturnMethodEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReturnMethodEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/RsvpResponseType
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RsvpResponseType {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/SizeGroupEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SizeGroupEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/SizeSystemEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SizeSystemEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/Specialty
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Specialty {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/StatusEnumeration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatusEnumeration {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/WarrantyScope
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WarrantyScope {
    #[serde(flatten)]
    pub enumeration: Enumeration,
}

/// https://schema.org/FloorPlan
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FloorPlan {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/GameServer
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GameServer {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/GeospatialGeometry
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeospatialGeometry {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Grant
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Grant {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/MonetaryGrant
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MonetaryGrant {
    #[serde(flatten)]
    pub grant: Grant,
}

/// https://schema.org/HealthInsurancePlan
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthInsurancePlan {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/HealthPlanCostSharingSpecification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthPlanCostSharingSpecification {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/HealthPlanFormulary
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthPlanFormulary {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/HealthPlanNetwork
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HealthPlanNetwork {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Invoice
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Invoice {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/ItemList
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ItemList {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/BreadcrumbList
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BreadcrumbList {
    #[serde(flatten)]
    pub item_list: ItemList,
}

/// https://schema.org/OfferCatalog
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OfferCatalog {
    #[serde(flatten)]
    pub item_list: ItemList,
}

/// https://schema.org/JobPosting
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobPosting {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Language
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Language {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/ListItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListItem {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/HowToItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HowToItem {
    #[serde(flatten)]
    pub list_item: ListItem,
}

/// https://schema.org/MediaSubscription
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MediaSubscription {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/MenuItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MenuItem {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/MerchantReturnPolicy
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MerchantReturnPolicy {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/MerchantReturnPolicySeasonalOverride
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MerchantReturnPolicySeasonalOverride {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Observation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Occupation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Occupation {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/OccupationalExperienceRequirements
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OccupationalExperienceRequirements {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Offer
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Offer {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/AggregateOffer
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggregateOffer {
    #[serde(flatten)]
    pub offer: Offer,
}

/// https://schema.org/OfferForLease
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OfferForLease {
    #[serde(flatten)]
    pub offer: Offer,
}

/// https://schema.org/OfferForPurchase
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OfferForPurchase {
    #[serde(flatten)]
    pub offer: Offer,
}

/// https://schema.org/Order
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/OrderItem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderItem {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/ParcelDelivery
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ParcelDelivery {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Permit
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Permit {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/GovernmentPermit
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GovernmentPermit {
    #[serde(flatten)]
    pub permit: Permit,
}

/// https://schema.org/ProgramMembership
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProgramMembership {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Property
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Property {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/PropertyValueSpecification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PropertyValueSpecification {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Quantity
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Quantity {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Distance
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Distance {
    #[serde(flatten)]
    pub quantity: Quantity,
}

/// https://schema.org/Duration
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Duration {
    #[serde(flatten)]
    pub quantity: Quantity,
}

/// https://schema.org/Energy
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Energy {
    #[serde(flatten)]
    pub quantity: Quantity,
}

/// https://schema.org/Mass
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Mass {
    #[serde(flatten)]
    pub quantity: Quantity,
}

/// https://schema.org/Rating
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Rating {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/AggregateRating
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AggregateRating {
    #[serde(flatten)]
    pub rating: Rating,
}

/// https://schema.org/EndorsementRating
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EndorsementRating {
    #[serde(flatten)]
    pub rating: Rating,
}

/// https://schema.org/Reservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Reservation {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/BoatReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BoatReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/BusReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BusReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/EventReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/FlightReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FlightReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/FoodEstablishmentReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FoodEstablishmentReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/LodgingReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LodgingReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/RentalCarReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RentalCarReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/ReservationPackage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReservationPackage {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/TaxiReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TaxiReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/TrainReservation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TrainReservation {
    #[serde(flatten)]
    pub reservation: Reservation,
}

/// https://schema.org/Role
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Role {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/LinkRole
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LinkRole {
    #[serde(flatten)]
    pub role: Role,
}

/// https://schema.org/OrganizationRole
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationRole {
    #[serde(flatten)]
    pub role: Role,
}

/// https://schema.org/PerformanceRole
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceRole {
    #[serde(flatten)]
    pub role: Role,
}

/// https://schema.org/Schedule
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Schedule {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Seat
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Seat {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Series
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Series {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/Service
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/ServiceChannel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServiceChannel {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/SpeakableSpecification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpeakableSpecification {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/StatisticalPopulation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatisticalPopulation {
    #[serde(flatten)]
    pub intangible: Intangible,
}

/// https://schema.org/StructuredValue
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StructuredValue {
    pub intangible: Intangible,
}

/// https://schema.org/Ticket
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Ticket {
    pub intangible: Intangible,
}

/// https://schema.org/Trip
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Trip {
    pub intangible: Intangible,
}

/// https://schema.org/VirtualLocation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VirtualLocation {
    pub intangible: Intangible,
}
/// https://schema.org/MedicalEntity
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalEntity {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/AnatomicalStructure
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AnatomicalStructure {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/AnatomicalSystem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AnatomicalSystem {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/DrugClass
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DrugClass {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/DrugCost
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DrugCost {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/LifestyleModification
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LifestyleModification {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalCause
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalCause {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalCondition
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalCondition {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalContraindication
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalContraindication {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalDevice
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalDevice {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalGuideline
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalGuideline {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalIndication
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalIndication {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalIntangible
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalIntangible {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalProcedure
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalProcedure {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalRiskEstimator
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalRiskEstimator {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalRiskFactor
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalRiskFactor {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalStudy
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalStudy {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/MedicalTest
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalTest {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/Substance
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Substance {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/SuperficialAnatomy
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SuperficialAnatomy {
    #[serde(flatten)]
    pub medical_entity: MedicalEntity,
}

/// https://schema.org/Organization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    #[serde(flatten)]
    pub extends: Thing,
    pub legal_name: Wrapper<Text>,
    pub email: Wrapper<Text>,
    pub keywords: Wrapper<Text>,
    pub address: Wrapper<PostalAddressOrText>,
}

/// https://schema.org/Airline
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Airline {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/Consortium
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Consortium {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/Corporation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Corporation {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/EducationalOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EducationalOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/FundingScheme
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingScheme {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/GovernmentOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GovernmentOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/LibrarySystem
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LibrarySystem {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/LocalBusiness
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LocalBusiness {
    #[serde(flatten)]
    pub organization: Organization,
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/MedicalOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MedicalOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/NGO
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::upper_case_acronyms)]
pub struct NGO {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/NewsMediaOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewsMediaOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/OnlineBusiness
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OnlineBusiness {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/PerfomingGroup
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PerfomingGroup {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/Project
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/ResearchOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResearchOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/SearchRescueOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SearchRescueOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/SportsOrganization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SportsOrganization {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/WorkersUnion
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkersUnion {
    #[serde(flatten)]
    pub organization: Organization,
}

/// https://schema.org/Person
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Person {
    #[serde(flatten)]
    pub extends: Thing,
    pub image: Wrapper<Text>,
    pub name: Wrapper<Text>,
    pub same_as: Wrapper<Text>,
}

/// https://schema.org/Patient
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Patient {
    #[serde(flatten)]
    pub person: Person,
}

/// https://schema.org/Place
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Place {
    #[serde(flatten)]
    pub thing: Thing,
    pub address: Wrapper<PostalAddressOrText>,
    pub telephone: Wrapper<Text>,
}

/// https://schema.org/Accommodation
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Accommodation {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/AdministrativeArea
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AdministrativeArea {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/CivicStructure
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CivicStructure {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/Landform
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Landform {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/LandmarksOrHistoricalBuildings
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LandmarksOrHistoricalBuildings {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/Residence
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Residence {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/TouristAttraction
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TouristAttraction {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/TouristDestination
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TouristDestination {
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/Product
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Product {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/DietarySupplement
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DietarySupplement {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/Drug
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Drug {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/IndividualProduct
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndividualProduct {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/ProductCollection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProductCollection {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/ProductGroup
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProductGroup {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/ProductModel
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProductModel {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/SomeProducts
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SomeProducts {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/Vehicle
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Vehicle {
    #[serde(flatten)]
    pub product: Product,
}

/// https://schema.org/Taxon
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Taxon {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/Country
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Country {
    #[serde(flatten)]
    pub administrative_area: AdministrativeArea,
}

/// https://schema.org/ContactPoint
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContactPoint {
    #[serde(flatten)]
    pub structured_value: StructuredValue,
}

/// https://schema.org/PostalAddress
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PostalAddress {
    #[serde(flatten)]
    pub contact_point: ContactPoint,
    pub address_country: Wrapper<CountryOrText>,
    pub address_locality: Wrapper<Text>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum CountryOrText {
    Country(Country),
    Text(Text),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum PostalAddressOrText {
    PostalAddress(PostalAddress),
    Text(Text),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum PersonOrOrganization {
    Person(Box<Person>),
    Name(Text),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_object_example() {
        // example taken from https://schema.org/ImageObject
        let json = r#"
        {
            "@context": "https://schema.org",
            "@type": "ImageObject",
            "author": "Jane Doe",
            "contentLocation": "Puerto Vallarta, Mexico",
            "contentUrl": "mexico-beach.jpg",
            "datePublished": "2008-01-25",
            "description": "I took this picture while on vacation last year.",
            "name": "Beach in Mexico"
          }
        "#;

        let parsed: SchemaOrg = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed,
            SchemaOrg::ImageObject(ImageObject {
                media_object: MediaObject {
                    creative_work: CreativeWork {
                        thing: Thing {
                            name: Some(OneOrMany::One(Box::new("Beach in Mexico".to_string()))),
                            description: Some(OneOrMany::One(Box::new(
                                "I took this picture while on vacation last year.".to_string()
                            ))),
                            ..Default::default()
                        },
                        author: Some(OneOrMany::One(Box::new(PersonOrOrganization::Name(
                            "Jane Doe".to_string()
                        )))),
                    },
                    content_url: Some(OneOrMany::One(Box::new("mexico-beach.jpg".to_string()))),
                }
            }),
        );
    }
}
